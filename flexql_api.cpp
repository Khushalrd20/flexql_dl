#include "flexql.h"
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <unistd.h>
#include <cstring>
#include <cstdlib>
#include <string>
#include <sstream>
#include <cstdint>

struct FlexQL {
    int sock;
};

int flexql_open(const char *host, int port, FlexQL **outDb)
{
    int s = socket(AF_INET, SOCK_STREAM, 0);
    if (s < 0) return FLEXQL_ERROR;

    // Disable Nagle for lower latency
    int flag = 1;
    setsockopt(s, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));

    // Increase socket buffer sizes for better throughput
    int send_buf = 16 * 1024 * 1024;  // 16 MB
    setsockopt(s, SOL_SOCKET, SO_SNDBUF, &send_buf, sizeof(send_buf));

    struct sockaddr_in serv{};
    serv.sin_family = AF_INET;
    serv.sin_port   = htons(port);
    if (inet_pton(AF_INET, host, &serv.sin_addr) <= 0) { close(s); return FLEXQL_ERROR; }

    if (connect(s, (struct sockaddr*)&serv, sizeof(serv)) < 0) { close(s); return FLEXQL_ERROR; }

    FlexQL *db = (FlexQL*)malloc(sizeof(FlexQL));
    if (!db) { close(s); return FLEXQL_ERROR; }
    db->sock = s;
    *outDb = db;
    return FLEXQL_OK;
}

int flexql_close(FlexQL *db)
{
    if (!db) return FLEXQL_ERROR;
    close(db->sock);
    free(db);
    return FLEXQL_OK;
}

// -------------------------------------------------------
// flexql_exec - TEXT-BASED SQL
// -------------------------------------------------------
int flexql_exec(
    FlexQL *db,
    const char *sql,
    int (*callback)(void*, int, char**, char**),
    void *arg,
    char **errmsg)
{
    if (!db || !sql) {
        if (errmsg) {
            const char *m = "null db or sql";
            *errmsg = (char*)malloc(strlen(m)+1);
            if (*errmsg) strcpy(*errmsg, m);
        }
        return FLEXQL_ERROR;
    }

    // Send SQL + '\n' terminator so server knows where query ends
    std::string sqlMsg = std::string(sql) + "\n";
    size_t len = sqlMsg.size();
    const char *sendbuf = sqlMsg.c_str();
    size_t sent = 0;
    while (sent < len) {
        int n = send(db->sock, sendbuf + sent, len - sent, MSG_NOSIGNAL);
        if (n <= 0) {
            if (errmsg) {
                const char *m = "send failed";
                *errmsg = (char*)malloc(strlen(m)+1);
                if (*errmsg) strcpy(*errmsg, m);
            }
            return FLEXQL_ERROR;
        }
        sent += n;
    }

    // Read response line by line until "END"
    std::string pending;
    char buf[8192];
    bool done = false;
    bool hasError = false;
    std::string errorText;

    while (!done) {
        int bytes = recv(db->sock, buf, sizeof(buf)-1, 0);
        if (bytes <= 0) break;
        pending.append(buf, bytes);

        size_t pos;
        while ((pos = pending.find('\n')) != std::string::npos) {
            std::string line = pending.substr(0, pos);
            pending.erase(0, pos + 1);

            // Strip trailing \r
            if (!line.empty() && line.back() == '\r') line.pop_back();

            if (line == "END") {
                done = true;
                break;
            }

            if (line.rfind("ERROR:", 0) == 0) {
                hasError = true;
                errorText = line;
                continue;
            }

            if (line.rfind("ROW ", 0) == 0 && callback) {
                std::string rowData = line.substr(4);

                char *argv_arr[1];
                char *col_arr[1];
                std::string colName = "row";
                argv_arr[0] = (char*)rowData.c_str();
                col_arr[0]  = (char*)colName.c_str();

                callback(arg, 1, argv_arr, col_arr);
            }
        }
    }

    if (!done) {
        if (errmsg) {
            const char *m = "connection closed before END";
            *errmsg = (char*)malloc(strlen(m)+1);
            if (*errmsg) strcpy(*errmsg, m);
        }
        return FLEXQL_ERROR;
    }

    if (hasError) {
        if (errmsg) {
            *errmsg = (char*)malloc(errorText.size()+1);
            if (*errmsg) strcpy(*errmsg, errorText.c_str());
        }
        return FLEXQL_ERROR;
    }

    return FLEXQL_OK;
}

void flexql_free(void *ptr)
{
    free(ptr);
}

// -------------------------------------------------------
// flexql_bulk_insert_binary - Tab-separated text binary format
// -------------------------------------------------------
int flexql_bulk_insert_binary(
    FlexQL *db,
    const char *table_name,
    const BulkRow *rows,
    int row_count,
    char **errmsg)
{
    if (!db || !table_name || !rows || row_count <= 0) {
        if (errmsg) {
            const char *m = "invalid parameters";
            *errmsg = (char*)malloc(strlen(m)+1);
            if (*errmsg) strcpy(*errmsg, m);
        }
        return FLEXQL_ERROR;
    }

    // Build binary bulk insert command
    std::string cmd = "BULK_BINARY\n";
    cmd += table_name;
    cmd += "\n";
    cmd += std::to_string(row_count);
    cmd += "\n";

    // Append all row data
    for (int i = 0; i < row_count; ++i) {
        const BulkRow &row = rows[i];
        for (int j = 0; j < row.col_count; ++j) {
            if (j > 0) cmd += "\t";
            if (row.values[j]) {
                cmd += row.values[j];
            }
        }
        cmd += "\n";
    }

    // Send command
    size_t len = cmd.size();
    const char *sendbuf = cmd.c_str();
    size_t sent = 0;
    while (sent < len) {
        int n = send(db->sock, sendbuf + sent, len - sent, MSG_NOSIGNAL);
        if (n <= 0) {
            if (errmsg) {
                const char *m = "send failed";
                *errmsg = (char*)malloc(strlen(m)+1);
                if (*errmsg) strcpy(*errmsg, m);
            }
            return FLEXQL_ERROR;
        }
        sent += n;
    }

    // Read response: "OK\nEND\n" or "ERROR: ...\nEND\n"
    std::string pending;
    char buf[8192];
    bool done = false;
    bool hasError = false;
    std::string errorText;

    while (!done) {
        int bytes = recv(db->sock, buf, sizeof(buf)-1, 0);
        if (bytes <= 0) break;
        pending.append(buf, bytes);

        size_t pos;
        while ((pos = pending.find('\n')) != std::string::npos) {
            std::string line = pending.substr(0, pos);
            pending.erase(0, pos + 1);

            if (!line.empty() && line.back() == '\r') line.pop_back();

            if (line == "END") {
                done = true;
                break;
            }

            if (line.rfind("ERROR:", 0) == 0) {
                hasError = true;
                errorText = line;
            }
        }
    }

    if (!done) {
        if (errmsg) {
            const char *m = "connection closed before END";
            *errmsg = (char*)malloc(strlen(m)+1);
            if (*errmsg) strcpy(*errmsg, m);
        }
        return FLEXQL_ERROR;
    }

    if (hasError) {
        if (errmsg) {
            *errmsg = (char*)malloc(errorText.size()+1);
            if (*errmsg) strcpy(*errmsg, errorText.c_str());
        }
        return FLEXQL_ERROR;
    }

    return FLEXQL_OK;
}

// -------------------------------------------------------
// flexql_bulk_insert_fixed_binary - ULTRA-FAST fixed binary format
// -------------------------------------------------------
int flexql_bulk_insert_fixed_binary(
    FlexQL *db,
    const char *table_name,
    const BulkRowBinary *rows,
    int row_count,
    char **errmsg)
{
    if (!db || !table_name || !rows || row_count <= 0) {
        if (errmsg) {
            const char *m = "invalid parameters";
            *errmsg = (char*)malloc(strlen(m)+1);
            if (*errmsg) strcpy(*errmsg, m);
        }
        return FLEXQL_ERROR;
    }

    // Build command header (text) + binary data
    std::string header = "BULK_FIXED\n";
    header += table_name;
    header += "\n";
    header += std::to_string(row_count);
    header += "\n";

    // Send header first
    size_t header_len = header.size();
    const char *header_buf = header.c_str();
    size_t sent = 0;
    while (sent < header_len) {
        int n = send(db->sock, header_buf + sent, header_len - sent, MSG_NOSIGNAL);
        if (n <= 0) {
            if (errmsg) {
                const char *m = "send failed";
                *errmsg = (char*)malloc(strlen(m)+1);
                if (*errmsg) strcpy(*errmsg, m);
            }
            return FLEXQL_ERROR;
        }
        sent += n;
    }

    // Send binary data (direct memory dump - NO CONVERSION!)
    size_t binary_len = (size_t)row_count * BULK_ROW_SIZE;
    const char *binary_buf = (const char*)rows;
    sent = 0;
    while (sent < binary_len) {
        int n = send(db->sock, binary_buf + sent, binary_len - sent, MSG_NOSIGNAL);
        if (n <= 0) {
            if (errmsg) {
                const char *m = "send failed";
                *errmsg = (char*)malloc(strlen(m)+1);
                if (*errmsg) strcpy(*errmsg, m);
            }
            return FLEXQL_ERROR;
        }
        sent += n;
    }

    // Send final newline to signal end
    if (send(db->sock, "\n", 1, MSG_NOSIGNAL) <= 0) {
        if (errmsg) {
            const char *m = "send failed";
            *errmsg = (char*)malloc(strlen(m)+1);
            if (*errmsg) strcpy(*errmsg, m);
        }
        return FLEXQL_ERROR;
    }

    // Read response
    std::string pending;
    char buf[8192];
    bool done = false;
    bool hasError = false;
    std::string errorText;

    while (!done) {
        int bytes = recv(db->sock, buf, sizeof(buf)-1, 0);
        if (bytes <= 0) break;
        pending.append(buf, bytes);

        size_t pos;
        while ((pos = pending.find('\n')) != std::string::npos) {
            std::string line = pending.substr(0, pos);
            pending.erase(0, pos + 1);

            if (!line.empty() && line.back() == '\r') line.pop_back();

            if (line == "END") {
                done = true;
                break;
            }

            if (line.rfind("ERROR:", 0) == 0) {
                hasError = true;
                errorText = line;
            }
        }
    }

    if (!done) {
        if (errmsg) {
            const char *m = "connection closed before END";
            *errmsg = (char*)malloc(strlen(m)+1);
            if (*errmsg) strcpy(*errmsg, m);
        }
        return FLEXQL_ERROR;
    }

    if (hasError) {
        if (errmsg) {
            *errmsg = (char*)malloc(errorText.size()+1);
            if (*errmsg) strcpy(*errmsg, errorText.c_str());
        }
        return FLEXQL_ERROR;
    }

    return FLEXQL_OK;
}
