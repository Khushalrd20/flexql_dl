#include <iostream>
#include <chrono>
#include <string>
#include <sstream>
#include <vector>
#include <thread>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include "flexql.h"

using namespace std;
using namespace std::chrono;

static const long long DEFAULT_INSERT_ROWS = 10000000LL;
static const int INSERT_BATCH_SIZE = 500000;   // 500K rows per batch
static const int NUM_PARALLEL_THREADS = 8;     // 8 parallel threads

struct QueryStats {
    long long rows = 0;
};

static int count_rows_callback(void *data, int argc, char **argv, char **azColName) {
    (void)argc;
    (void)argv;
    (void)azColName;
    QueryStats *stats = static_cast<QueryStats*>(data);
    if (stats) {
        stats->rows++;
    }
    return 0;
}

static bool run_exec(FlexQL *db, const string &sql, const string &label) {
    char *errMsg = nullptr;
    auto start = high_resolution_clock::now();
    int rc = flexql_exec(db, sql.c_str(), nullptr, nullptr, &errMsg);
    auto end = high_resolution_clock::now();
    long long elapsed = duration_cast<milliseconds>(end - start).count();

    if (rc != FLEXQL_OK) {
        cout << "[FAIL] " << label << " -> " << (errMsg ? errMsg : "unknown error") << "\n";
        if (errMsg) {
            flexql_free(errMsg);
        }
        return false;
    }

    cout << "[PASS] " << label << " (" << elapsed << " ms)\n";
    return true;
}

static bool run_query(FlexQL *db, const string &sql, const string &label) {
    QueryStats stats;
    char *errMsg = nullptr;
    auto start = high_resolution_clock::now();
    int rc = flexql_exec(db, sql.c_str(), count_rows_callback, &stats, &errMsg);
    auto end = high_resolution_clock::now();
    long long elapsed = duration_cast<milliseconds>(end - start).count();

    if (rc != FLEXQL_OK) {
        cout << "[FAIL] " << label << " -> " << (errMsg ? errMsg : "unknown error") << "\n";
        if (errMsg) {
            flexql_free(errMsg);
        }
        return false;
    }

    cout << "[PASS] " << label << " | rows=" << stats.rows << " | " << elapsed << " ms\n";
    return true;
}

static bool query_rows(FlexQL *db, const string &sql, vector<string> &out_rows) {
    struct Collector {
        vector<string> rows;
    } collector;

    auto cb = [](void *data, int argc, char **argv, char **azColName) -> int {
        (void)azColName;
        Collector *c = static_cast<Collector*>(data);
        string row;
        for (int i = 0; i < argc; ++i) {
            if (i > 0) {
                row += "|";
            }
            row += (argv[i] ? argv[i] : "NULL");
        }
        c->rows.push_back(row);
        return 0;
    };

    char *errMsg = nullptr;
    int rc = flexql_exec(db, sql.c_str(), cb, &collector, &errMsg);
    if (rc != FLEXQL_OK) {
        cout << "[FAIL] " << sql << " -> " << (errMsg ? errMsg : "unknown error") << "\n";
        if (errMsg) {
            flexql_free(errMsg);
        }
        return false;
    }

    out_rows = collector.rows;
    return true;
}

static bool assert_rows_equal(const string &label, const vector<string> &actual, const vector<string> &expected) {
    if (actual == expected) {
        cout << "[PASS] " << label << "\n";
        return true;
    }

    cout << "[FAIL] " << label << "\n";
    cout << "Expected (" << expected.size() << "):\n";
    for (const auto &r : expected) {
        cout << "  " << r << "\n";
    }
    cout << "Actual (" << actual.size() << "):\n";
    for (const auto &r : actual) {
        cout << "  " << r << "\n";
    }
    return false;
}

static bool expect_query_failure(FlexQL *db, const string &sql, const string &label) {
    char *errMsg = nullptr;
    int rc = flexql_exec(db, sql.c_str(), nullptr, nullptr, &errMsg);
    if (rc == FLEXQL_OK) {
        cout << "[FAIL] " << label << " (expected failure, got success)\n";
        return false;
    }
    if (errMsg) {
        flexql_free(errMsg);
    }
    cout << "[PASS] " << label << "\n";
    return true;
}

static bool assert_row_count(const string &label, const vector<string> &rows, size_t expected_count) {
    if (rows.size() == expected_count) {
        cout << "[PASS] " << label << "\n";
        return true;
    }

    cout << "[FAIL] " << label << " (expected " << expected_count << ", got " << rows.size() << ")\n";
    return false;
}

static bool run_data_level_unit_tests(FlexQL *db) {
    cout << "\n\n[[ Running Unit Tests ]]\n\n";

    bool all_ok = true;
    int total_tests = 0;
    int failed_tests = 0;

    auto record = [&](bool result) {
        total_tests++;
        if (!result) {
            all_ok = false;
            failed_tests++;
        }
    };

    record(run_exec(
            db,
            "CREATE TABLE IF NOT EXISTS TEST_USERS(ID DECIMAL, NAME VARCHAR(64), BALANCE DECIMAL, EXPIRES_AT DECIMAL);",
            "CREATE TABLE TEST_USERS"));

    record(run_exec(db, "DELETE FROM TEST_USERS;", "RESET TEST_USERS"));

    record(run_exec(
            db,
            "INSERT INTO TEST_USERS VALUES "
            "(1, 'Alice', 1200, 1893456000),"
            "(2, 'Bob', 450, 1893456000),"
            "(3, 'Carol', 2200, 1893456000),"
            "(4, 'Dave', 800, 1893456000);",
            "INSERT TEST_USERS"));

    vector<string> rows;

    bool q1 = query_rows(db, "SELECT NAME, BALANCE FROM TEST_USERS WHERE ID = 2;", rows);
    record(q1);
    if (q1) {
        record(assert_rows_equal("Single-row value validation", rows, {"Bob 450"}));
    }

    bool q2 = query_rows(db, "SELECT NAME FROM TEST_USERS WHERE BALANCE > 1000 ORDER BY NAME;", rows);
    record(q2);
    if (q2) {
        record(assert_rows_equal("Filtered rows validation", rows, {"Alice", "Carol"}));
    }

    bool q3 = query_rows(db, "SELECT NAME FROM TEST_USERS ORDER BY BALANCE DESC;", rows);
    record(q3);
    if (q3) {
        record(assert_rows_equal("ORDER BY descending validation", rows, {"Carol", "Alice", "Dave", "Bob"}));
    }

    bool q4 = query_rows(db, "SELECT ID FROM TEST_USERS WHERE BALANCE > 5000;", rows);
    record(q4);
    if (q4) {
        record(assert_row_count("Empty result-set validation", rows, 0));
    }

    record(run_exec(
            db,
            "CREATE TABLE IF NOT EXISTS TEST_ORDERS(ORDER_ID DECIMAL, USER_ID DECIMAL, AMOUNT DECIMAL, EXPIRES_AT DECIMAL);",
            "CREATE TABLE TEST_ORDERS"));

    record(run_exec(db, "DELETE FROM TEST_ORDERS;", "RESET TEST_ORDERS"));

    record(run_exec(
            db,
            "INSERT INTO TEST_ORDERS VALUES "
            "(101, 1, 50, 1893456000),"
            "(102, 1, 150, 1893456000),"
            "(103, 3, 500, 1893456000);",
            "INSERT TEST_ORDERS"));

    bool q5 = query_rows(
            db,
            "SELECT TEST_USERS.NAME, TEST_ORDERS.AMOUNT "
            "FROM TEST_USERS INNER JOIN TEST_ORDERS ON TEST_USERS.ID = TEST_ORDERS.USER_ID "
            "WHERE TEST_ORDERS.AMOUNT >= 100 ORDER BY TEST_ORDERS.ORDER_ID;",
            rows);
    record(q5);
    if (q5) {
        record(assert_rows_equal("Join result validation", rows, {"Alice 150", "Carol 500"}));
    }

    bool q6 = query_rows(db, "SELECT ORDER_ID FROM TEST_ORDERS WHERE USER_ID = 1 ORDER BY ORDER_ID;", rows);
    record(q6);
    if (q6) {
        record(assert_rows_equal("Single-condition equality WHERE validation", rows, {"101", "102"}));
    }

    bool q7 = query_rows(
            db,
            "SELECT TEST_USERS.NAME, TEST_ORDERS.AMOUNT "
            "FROM TEST_USERS INNER JOIN TEST_ORDERS ON TEST_USERS.ID = TEST_ORDERS.USER_ID "
            "WHERE TEST_ORDERS.AMOUNT > 900;",
            rows);
    record(q7);
    if (q7) {
        record(assert_row_count("Join with no matches validation", rows, 0));
    }

    record(expect_query_failure(db, "SELECT UNKNOWN_COLUMN FROM TEST_USERS;", "Invalid SQL should fail"));
    record(expect_query_failure(db, "SELECT * FROM MISSING_TABLE;", "Missing table should fail"));

    int passed_tests = total_tests - failed_tests;
    cout << "\nUnit Test Summary: " << passed_tests << "/" << total_tests << " passed, "
         << failed_tests << " failed.\n\n";

    return all_ok;
}

static bool run_insert_benchmark(FlexQL *db, long long target_rows) {
    if (!run_exec(
            db,
            "CREATE TABLE BIG_USERS(ID DECIMAL, NAME VARCHAR(64), EMAIL VARCHAR(64), BALANCE DECIMAL, EXPIRES_AT DECIMAL);",
            "CREATE TABLE BIG_USERS")) {
        return false;
    }

    cout << "\nStarting insertion benchmark for " << target_rows << " rows...\n";

    auto bench_start = high_resolution_clock::now();
    atomic<long long> total_inserted(0);
    atomic<bool> error_occurred(false);
    long long next_progress = 100000;  // Report every 100K rows

    // Pre-allocate rows buffer for each thread batch
    auto insert_thread_func = [&](int thread_id, long long start_id, long long end_id) {
        FlexQL *thread_db = nullptr;
        if (flexql_open("127.0.0.1", 9000, &thread_db) != FLEXQL_OK) {
            error_occurred = true;
            return;
        }

        // Prepare reusable buffer for fixed-format rows
        vector<BulkRowBinary> binary_rows;
        binary_rows.reserve(INSERT_BATCH_SIZE);

        long long current_id = start_id;
        while (current_id < end_id && !error_occurred) {
            int batch_size = min((long long)INSERT_BATCH_SIZE, end_id - current_id);
            
            binary_rows.clear();

            // Prepare fixed-format row data (NO STRING OPERATIONS!)
            for (int i = 0; i < batch_size; ++i) {
                long long id = current_id + i;
                
                BulkRowBinary row;
                row.id = (uint64_t)id;
                row.balance = (uint64_t)(1000 + (id % 10000));
                row.expires = 1893456000ULL;
                
                // Use snprintf for minimal overhead
                snprintf(row.name, sizeof(row.name), "user%lld", (long long)id);
                snprintf(row.email, sizeof(row.email), "user%lld@mail.com", (long long)id);
                
                binary_rows.push_back(row);
            }

            // Send bulk insert with fixed binary format
            char *errMsg = nullptr;
            if (flexql_bulk_insert_fixed_binary(thread_db, "BIG_USERS", 
                    binary_rows.data(), batch_size, &errMsg) != FLEXQL_OK) {
                if (errMsg) flexql_free(errMsg);
                error_occurred = true;
                flexql_close(thread_db);
                return;
            }

            total_inserted += batch_size;
            current_id += batch_size;
        }

        flexql_close(thread_db);
    };

    // Launch parallel insertion threads
    vector<thread> threads;
    long long rows_per_thread = target_rows / NUM_PARALLEL_THREADS;
    
    for (int i = 0; i < NUM_PARALLEL_THREADS; ++i) {
        long long start_id = i * rows_per_thread + 1;
        long long end_id = (i == NUM_PARALLEL_THREADS - 1) ? target_rows + 1 : (i + 1) * rows_per_thread + 1;
        threads.emplace_back(insert_thread_func, i, start_id, end_id);
    }

    // Monitor progress and report every 100K rows
    while (total_inserted.load() < target_rows && !error_occurred) {
        if (total_inserted.load() >= next_progress) {
            cout << "Progress: " << next_progress << "/" << target_rows << "\n";
            next_progress += 100000;
        }
        this_thread::sleep_for(milliseconds(10));
    }

    // Wait for all threads to complete
    for (auto &t : threads) {
        t.join();
    }

    // Show final progress
    long long final_inserted = total_inserted.load();
    while (next_progress <= final_inserted && next_progress <= target_rows) {
        cout << "Progress: " << next_progress << "/" << target_rows << "\n";
        next_progress += 100000;
    }

    if (error_occurred) {
        cout << "[FAIL] Insertion benchmark failed\n";
        return false;
    }

    auto bench_end = high_resolution_clock::now();
    long long elapsed = duration_cast<milliseconds>(bench_end - bench_start).count();
    long long throughput = (elapsed > 0) ? (total_inserted.load() * 1000LL / elapsed) : 0;

    cout << "[PASS] INSERT benchmark complete\n";
    cout << "Rows inserted: " << total_inserted.load() << "\n";
    cout << "Elapsed: " << elapsed << " ms\n";
    cout << "Throughput: " << throughput << " rows/sec\n";

    return total_inserted.load() == target_rows;
}

int main(int argc, char **argv) {
    FlexQL *db = nullptr;
    long long insert_rows = DEFAULT_INSERT_ROWS;
    bool run_unit_tests_only = false;

    if (argc > 1) {
        string arg1 = argv[1];
        if (arg1 == "--unit-test") {
            run_unit_tests_only = true;
        } else {
            insert_rows = atoll(argv[1]);
            if (insert_rows <= 0) {
                cout << "Invalid row count. Use a positive integer or --unit-test.\n";
                return 1;
            }
        }
    }

    if (flexql_open("127.0.0.1", 9000, &db) != FLEXQL_OK) {
        cout << "Cannot open FlexQL\n";
        return 1;
    }

    cout << "Connected to FlexQL\n";

    if (run_unit_tests_only) {
        bool ok = run_data_level_unit_tests(db);
        flexql_close(db);
        return ok ? 0 : 1;
    }

    cout << "Running SQL subset checks plus insertion benchmark...\n";
    cout << "Target insert rows: " << insert_rows << "\n\n";

    if (!run_insert_benchmark(db, insert_rows)) {
        flexql_close(db);
        return 1;
    }


    if (!run_data_level_unit_tests(db)) {
        flexql_close(db);
        return 1;
    }

    flexql_close(db);
    return 0;
}   