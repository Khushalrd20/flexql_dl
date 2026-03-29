#include "parser.h"
#include <sstream>
#include <algorithm>

//---------------------------------------------
// TRIM FUNCTION
//---------------------------------------------
static std::string trim(const std::string &s)
{
    size_t start = s.find_first_not_of(" \t\n\r");
    size_t end = s.find_last_not_of(" \t\n\r");
    return (start == std::string::npos) ? "" : s.substr(start, end - start + 1);
}

//---------------------------------------------
// MAIN EXECUTION
//---------------------------------------------
std::string executeQuery(Database &db, std::string query)
{
    query = trim(query);

    if (!query.empty() && query.back() == ';')
        query.pop_back();

    //-----------------------------------------
    // CREATE TABLE
    //-----------------------------------------
    if (query.find("CREATE TABLE") == 0)
    {
        size_t start = query.find("TABLE") + 5;
        size_t paren = query.find("(");

        std::string table = trim(query.substr(start, paren - start));

        size_t end = query.find(")");
        std::string colStr = query.substr(paren + 1, end - paren - 1);

        std::stringstream ss(colStr);
        std::string col;
        std::vector<std::string> cols;

        while (getline(ss, col, ','))
        {
            cols.push_back(trim(col));
        }

        return db.createTable(table, cols) + "END\n";
    }

    //-----------------------------------------
    // DELETE (FULL TABLE)
    //-----------------------------------------
    if (query.find("DELETE FROM") == 0)
    {
        size_t start = query.find("FROM") + 4;
        std::string table = trim(query.substr(start));

        if (db.tables.find(table) == db.tables.end())
            return "ERROR: table not found\nEND\n";

        db.tables[table].rows.clear();
        db.tables[table].primaryIndex.clear();

        return "OK\nEND\n";
    }

    //-----------------------------------------
    // INSERT INTO
    //-----------------------------------------
    if (query.find("INSERT INTO") == 0)
    {
        size_t intoPos = query.find("INTO") + 4;
        size_t valuesPos = query.find("VALUES");

        if (valuesPos == std::string::npos)
            return "ERROR: VALUES keyword missing\nEND\n";

        std::string table = trim(query.substr(intoPos, valuesPos - intoPos));

        if (db.tables.find(table) == db.tables.end())
            return "ERROR: table not found\nEND\n";

        std::vector<std::vector<std::string>> rows;

        size_t pos = valuesPos;

        while (true)
        {
            size_t start = query.find("(", pos);
            if (start == std::string::npos) break;

            size_t end = query.find(")", start);
            if (end == std::string::npos) break;

            std::string row = query.substr(start + 1, end - start - 1);

            std::stringstream ss(row);
            std::string val;
            std::vector<std::string> vals;

            while (getline(ss, val, ','))
            {
                val = trim(val);

                // remove quotes
                if (!val.empty() && val.front() == '\'') val.erase(0, 1);
                if (!val.empty() && val.back() == '\'') val.pop_back();

                vals.push_back(val);
            }

            rows.push_back(vals);
            pos = end + 1;
        }

        db.bulkInsert(table, rows);

        return "OK\nEND\n";
    }

    //-----------------------------------------
    // SELECT *
    //-----------------------------------------
    if (query.find("SELECT") == 0)
    {
        size_t fromPos = query.find("FROM");
        if (fromPos == std::string::npos)
            return "ERROR: missing FROM\nEND\n";

        std::string table = trim(query.substr(fromPos + 4));

        if (db.tables.find(table) == db.tables.end())
            return "ERROR: table not found\nEND\n";

        Table &t = db.tables[table];

        std::stringstream out;

        for (auto &row : t.rows)
        {
            for (auto &v : row.values)
                out << v << " ";
            out << "\n";
        }

        out << "END\n";
        return out.str();
    }

    //-----------------------------------------
    // UNKNOWN
    //-----------------------------------------
    return "ERROR: Invalid query\nEND\n";
}