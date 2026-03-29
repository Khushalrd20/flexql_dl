#pragma once
#include "database.h"
#include <string>
#include <vector>
#include <sstream>
#include <algorithm>
#include <cctype>
#include <ctime>

//---------------------------------------------
// STRING UTILITIES
//---------------------------------------------
static inline std::string toUpper(std::string s) {
    for (auto &c : s) c = toupper((unsigned char)c);
    return s;
}

static inline std::string trim(const std::string &s) {
    size_t a = s.find_first_not_of(" \t\r\n");
    size_t b = s.find_last_not_of(" \t\r\n");
    if (a == std::string::npos) return "";
    return s.substr(a, b - a + 1);
}

static inline std::string stripQuotes(const std::string &s) {
    if (s.size() >= 2 &&
        ((s.front() == '\'' && s.back() == '\'') ||
         (s.front() == '"'  && s.back() == '"'))) {
        return s.substr(1, s.size() - 2);
    }
    return s;
}

// Split by delimiter, respecting single-quoted strings
static std::vector<std::string> splitRespectingQuotes(const std::string &s, char delim) {
    std::vector<std::string> result;
    std::string cur;
    bool inQuote = false;
    for (size_t i = 0; i < s.size(); i++) {
        char c = s[i];
        if (c == '\'' && !inQuote) { inQuote = true;  cur += c; }
        else if (c == '\'' && inQuote) { inQuote = false; cur += c; }
        else if (c == delim && !inQuote) {
            result.push_back(trim(cur)); cur.clear();
        } else {
            cur += c;
        }
    }
    if (!cur.empty() || !result.empty()) result.push_back(trim(cur));
    return result;
}

//---------------------------------------------
// TOKENIZER
//---------------------------------------------
static std::vector<std::string> tokenize(const std::string &sql) {
    std::vector<std::string> tokens;
    std::string cur;
    bool inQ = false;
    for (size_t i = 0; i < sql.size(); i++) {
        char c = sql[i];
        if (c == '\'' && !inQ) { inQ = true; cur += c; }
        else if (c == '\'' && inQ) { inQ = false; cur += c; }
        else if (inQ) { cur += c; }
        else if (c == '(' || c == ')' || c == ',' || c == ';') {
            if (!cur.empty()) { tokens.push_back(trim(cur)); cur.clear(); }
            tokens.push_back(std::string(1, c));
        } else if (c == '>' || c == '<' || c == '!' || c == '=') {
            if (!cur.empty()) { tokens.push_back(trim(cur)); cur.clear(); }
            // Peek for two-char ops: >=, <=, !=
            if (i + 1 < sql.size() && sql[i+1] == '=') {
                tokens.push_back(std::string(1, c) + "=");
                i++;
            } else {
                tokens.push_back(std::string(1, c));
            }
        } else if (isspace((unsigned char)c)) {
            if (!cur.empty()) { tokens.push_back(trim(cur)); cur.clear(); }
        } else {
            cur += c;
        }
    }
    if (!cur.empty()) tokens.push_back(trim(cur));
    return tokens;
}

//---------------------------------------------
// FIND COLUMN INDEX (case-insensitive)
// Supports "TABLE.COLUMN" or just "COLUMN"
//---------------------------------------------
static int findColIdx(const Table &t, const std::string &colRef) {
    std::string up = toUpper(colRef);
    // strip table prefix
    size_t dot = up.find('.');
    std::string colName = (dot != std::string::npos) ? up.substr(dot + 1) : up;
    for (int i = 0; i < (int)t.columns.size(); i++) {
        std::string tc = toUpper(t.columns[i]);
        size_t tdot = tc.find('.');
        std::string tcName = (tdot != std::string::npos) ? tc.substr(tdot + 1) : tc;
        if (tcName == colName) return i;
    }
    return -1;
}

//---------------------------------------------
// COMPARE VALUES for WHERE clause
//---------------------------------------------
static bool compareValues(const std::string &a, const std::string &b, const std::string &op) {
    // Try numeric compare
    bool aNum = false, bNum = false;
    double da = 0, db = 0;
    try { da = std::stod(a); aNum = true; } catch (...) {}
    try { db = std::stod(b); bNum = true; } catch (...) {}

    if (aNum && bNum) {
        if (op == "=")  return da == db;
        if (op == ">")  return da > db;
        if (op == "<")  return da < db;
        if (op == ">=") return da >= db;
        if (op == "<=") return da <= db;
        if (op == "!=") return da != db;
    }
    // String compare
    if (op == "=")  return a == b;
    if (op == "!=") return a != b;
    if (op == ">")  return a > b;
    if (op == "<")  return a < b;
    return false;
}

//---------------------------------------------
// EXECUTE: CREATE TABLE
// CREATE TABLE [IF NOT EXISTS] name (col TYPE, ...)
//---------------------------------------------
static std::string execCreate(Database &db, const std::string &sql) {
    // Parse tokens
    std::vector<std::string> toks = tokenize(sql);
    // Find table name: after CREATE TABLE [IF NOT EXISTS]
    size_t i = 0;
    while (i < toks.size() && toUpper(toks[i]) != "TABLE") i++;
    i++; // skip TABLE
    if (i >= toks.size()) return "ERROR: syntax error in CREATE TABLE\n";

    // IF NOT EXISTS
    bool ifNotExists = false;
    if (i < toks.size() && toUpper(toks[i]) == "IF") {
        ifNotExists = true;
        while (i < toks.size() && toUpper(toks[i]) != "EXISTS") i++;
        i++; // skip EXISTS
    }

    if (i >= toks.size()) return "ERROR: missing table name\n";
    std::string tableName = toUpper(toks[i]); i++;

    if (ifNotExists && db.tables.count(tableName)) {
        return "Table already exists\n";
    }

    // Collect columns between ( )
    // Find opening paren
    while (i < toks.size() && toks[i] != "(") i++;
    i++; // skip (

    std::vector<std::string> cols;
    while (i < toks.size() && toks[i] != ")") {
        std::string colName = toUpper(toks[i]); i++;
        // Skip type and any modifiers (including nested parens like VARCHAR(64))
        // Stop at a top-level ',' or ')' — track paren depth so VARCHAR(64)'s ')'
        // does NOT end the column list.
        int depth = 0;
        while (i < toks.size()) {
            if (toks[i] == "(") { depth++; i++; continue; }
            if (toks[i] == ")") {
                if (depth > 0) { depth--; i++; continue; }
                break; // top-level ')' = end of column list
            }
            if (toks[i] == "," && depth == 0) break; // top-level ',' = next column
            i++;
        }
        if (i < toks.size() && toks[i] == ",") i++; // consume the comma
        cols.push_back(colName);
    }

    if (cols.empty()) return "ERROR: no columns defined\n";

    Table t;
    t.name = tableName;
    t.columns = cols;
    db.tables[tableName] = t;
    return "OK\n";
}

//---------------------------------------------
// EXECUTE: DROP TABLE
//---------------------------------------------
static std::string execDrop(Database &db, const std::string &sql) {
    std::vector<std::string> toks = tokenize(sql);
    size_t i = 0;
    while (i < toks.size() && toUpper(toks[i]) != "TABLE") i++;
    i++;
    if (i >= toks.size()) return "ERROR: missing table name\n";
    std::string tName = toUpper(toks[i]);
    if (!db.tables.count(tName)) return "ERROR: table not found\n";
    db.tables.erase(tName);
    return "OK\n";
}

//---------------------------------------------
// EXECUTE: DELETE FROM
//---------------------------------------------
static std::string execDelete(Database &db, const std::string &sql) {
    // DELETE FROM table [WHERE col op val]
    std::vector<std::string> toks = tokenize(sql);
    size_t i = 0;
    while (i < toks.size() && toUpper(toks[i]) != "FROM") i++;
    i++;
    if (i >= toks.size()) return "ERROR: missing table name\n";
    std::string tName = toUpper(toks[i]); i++;

    if (!db.tables.count(tName)) return "ERROR: table not found: " + tName + "\n";
    Table &t = db.tables[tName];

    // Check for WHERE
    bool hasWhere = false;
    std::string whereCol, whereOp, whereVal;
    while (i < toks.size()) {
        if (toUpper(toks[i]) == "WHERE") {
            hasWhere = true; i++;
            if (i >= toks.size()) return "ERROR: incomplete WHERE\n";
            whereCol = toUpper(toks[i]); i++;
            if (i >= toks.size()) return "ERROR: missing operator\n";
            whereOp = toks[i]; i++;
            if (i >= toks.size()) return "ERROR: missing value\n";
            whereVal = stripQuotes(toks[i]); i++;
        } else {
            i++;
        }
    }

    if (!hasWhere) {
        // DELETE all
        t.rows.clear();
        t.primaryIndex.clear();
        return "OK\n";
    }

    int ci = findColIdx(t, whereCol);
    if (ci == -1) return "ERROR: column not found: " + whereCol + "\n";

    std::vector<Row> newRows;
    for (auto &r : t.rows) {
        if (compareValues(r.values[ci], whereVal, whereOp)) continue;
        newRows.push_back(r);
    }
    // Rebuild index
    t.rows = newRows;
    t.primaryIndex.clear();
    for (size_t j = 0; j < t.rows.size(); j++)
        t.primaryIndex[t.rows[j].values[0]] = (int)j;
    return "OK\n";
}

//---------------------------------------------
// EXECUTE: INSERT INTO table VALUES (...)
// Supports multi-row: VALUES (...), (...), ...
//---------------------------------------------
static std::string execInsert(Database &db, const std::string &sql) {
    std::vector<std::string> toks = tokenize(sql);
    size_t i = 0;
    while (i < toks.size() && toUpper(toks[i]) != "INTO") i++;
    i++; // skip INTO
    if (i >= toks.size()) return "ERROR: missing table name\n";
    std::string tName = toUpper(toks[i]); i++;

    if (!db.tables.count(tName)) return "ERROR: table not found: " + tName + "\n";
    Table &t = db.tables[tName];

    // Skip to VALUES
    while (i < toks.size() && toUpper(toks[i]) != "VALUES") i++;
    i++; // skip VALUES

    int inserted = 0;
    // Parse multiple value groups: (v1,v2,...), (v1,v2,...), ...
    while (i < toks.size()) {
        if (toks[i] == ";") break;
        if (toks[i] == ",") { i++; continue; } // between groups
        if (toks[i] != "(") { i++; continue; }
        i++; // skip (

        std::vector<std::string> vals;
        while (i < toks.size() && toks[i] != ")") {
            if (toks[i] != ",")
                vals.push_back(stripQuotes(toks[i]));
            i++;
        }
        if (i < toks.size()) i++; // skip )

        if (vals.size() != t.columns.size())
            return "ERROR: column count mismatch (expected " +
                   std::to_string(t.columns.size()) + ", got " +
                   std::to_string(vals.size()) + ")\n";

        std::string pk = vals[0];
        if (t.primaryIndex.count(pk))
            return "ERROR: duplicate primary key: " + pk + "\n";

        Row r;
        r.expiry = time(nullptr) + 3600;
        r.values = vals;
        t.primaryIndex[pk] = (int)t.rows.size();
        t.rows.push_back(std::move(r));
        inserted++;
    }

    return "OK\n";
}

//---------------------------------------------
// FORMAT ROW for output
// Returns space-separated selected column values
//---------------------------------------------
static std::string formatRow(const std::vector<std::string> &vals,
                             const std::vector<int> &colIdxs) {
    std::string out;
    for (size_t i = 0; i < colIdxs.size(); i++) {
        if (i > 0) out += " ";
        out += vals[colIdxs[i]];
    }
    return out;
}

//---------------------------------------------
// PARSE SELECT columns list
// Returns column names; "*" stays as-is
//---------------------------------------------
static std::vector<std::string> parseSelectCols(const std::string &colStr) {
    if (trim(colStr) == "*") return {"*"};
    std::vector<std::string> cols;
    std::vector<std::string> parts = splitRespectingQuotes(colStr, ',');
    for (auto &p : parts) cols.push_back(toUpper(trim(p)));
    return cols;
}

//---------------------------------------------
// EXECUTE: SELECT (single table + optional JOIN)
//---------------------------------------------
static std::string execSelect(Database &db, const std::string &sql) {
    // Uppercase for keyword matching, but preserve original for values
    std::string up = sql;
    for (auto &c : up) c = toupper((unsigned char)c);

    // --- Parse column list ---
    size_t selPos = up.find("SELECT");
    size_t fromPos = up.find(" FROM ");
    if (selPos == std::string::npos || fromPos == std::string::npos)
        return "ERROR: invalid SELECT syntax\n";

    std::string colStr = trim(sql.substr(selPos + 6, fromPos - selPos - 6));
    std::vector<std::string> selColNames = parseSelectCols(colStr);
    for (auto &c : selColNames) c = toUpper(c);

    // --- Parse FROM clause (table name) ---
    size_t afterFrom = fromPos + 6;

    // Check for JOIN
    size_t joinPos = up.find(" INNER JOIN ", afterFrom);
    size_t wherePos = up.find(" WHERE ", afterFrom);
    size_t orderPos = up.find(" ORDER BY ", afterFrom);

    bool hasJoin = (joinPos != std::string::npos);

    // =========================================================
    // SINGLE TABLE SELECT
    // =========================================================
    if (!hasJoin) {
        // Table name ends at WHERE, ORDER BY, or end
        size_t tEnd = std::string::npos;
        if (wherePos != std::string::npos) tEnd = wherePos;
        if (orderPos != std::string::npos && (tEnd == std::string::npos || orderPos < tEnd)) tEnd = orderPos;

        std::string tName = toUpper(trim(
            tEnd == std::string::npos ? sql.substr(afterFrom) : sql.substr(afterFrom, tEnd - afterFrom)
        ));
        // Remove trailing semicolon
        if (!tName.empty() && tName.back() == ';') tName.pop_back();
        tName = trim(tName);

        if (!db.tables.count(tName))
            return "ERROR: table not found: " + tName + "\n";
        Table &t = db.tables[tName];

        // Resolve column indices
        std::vector<int> colIdxs;
        if (selColNames.size() == 1 && selColNames[0] == "*") {
            for (int i = 0; i < (int)t.columns.size(); i++) colIdxs.push_back(i);
        } else {
            for (auto &cn : selColNames) {
                int idx = findColIdx(t, cn);
                if (idx == -1) return "ERROR: column not found: " + cn + "\n";
                colIdxs.push_back(idx);
            }
        }

        // Parse WHERE
        bool hasWhere = (wherePos != std::string::npos);
        std::string wCol, wOp, wVal;
        if (hasWhere) {
            size_t wStart = wherePos + 7;
            size_t wEnd = (orderPos != std::string::npos && orderPos > wherePos) ? orderPos : std::string::npos;
            std::string wClause = trim(wEnd == std::string::npos ? sql.substr(wStart) : sql.substr(wStart, wEnd - wStart));
            if (!wClause.empty() && wClause.back() == ';') wClause.pop_back();
            wClause = trim(wClause);

            // Parse: col OP val
            // OP can be >=, <=, !=, =, >, <
            std::vector<std::string> wToks = tokenize(wClause);
            if (wToks.size() < 3) return "ERROR: incomplete WHERE clause\n";
            wCol = toUpper(wToks[0]);
            wOp  = wToks[1];
            wVal = stripQuotes(wToks[2]);

            int ci = findColIdx(t, wCol);
            if (ci == -1) return "ERROR: column not found: " + wCol + "\n";
        }

        // Parse ORDER BY
        bool hasOrder = (orderPos != std::string::npos);
        std::string orderCol;
        bool orderDesc = false;
        if (hasOrder) {
            size_t oStart = orderPos + 10;
            std::string oClause = trim(sql.substr(oStart));
            if (!oClause.empty() && oClause.back() == ';') oClause.pop_back();
            oClause = trim(oClause);
            std::vector<std::string> oToks = tokenize(oClause);
            if (!oToks.empty()) {
                orderCol = toUpper(oToks[0]);
                if (oToks.size() > 1 && toUpper(oToks[1]) == "DESC") orderDesc = true;
            }
        }

        // Build result rows
        int whereColIdx = -1;
        if (hasWhere) {
            whereColIdx = findColIdx(t, wCol);
            if (whereColIdx == -1) return "ERROR: column not found: " + wCol + "\n";
        }
        int orderColIdx = -1;
        if (hasOrder) {
            orderColIdx = findColIdx(t, orderCol);
            if (orderColIdx == -1) return "ERROR: column not found: " + orderCol + "\n";
        }

        // Filter
        std::vector<const Row*> result;
        time_t now = time(nullptr);
        for (const auto &r : t.rows) {
            if (now > r.expiry) continue;
            if (hasWhere && !compareValues(r.values[whereColIdx], wVal, wOp)) continue;
            result.push_back(&r);
        }

        // Sort
        if (hasOrder) {
            std::stable_sort(result.begin(), result.end(),
                [&](const Row *a, const Row *b) {
                    const std::string &va = a->values[orderColIdx];
                    const std::string &vb = b->values[orderColIdx];
                    // try numeric
                    double da = 0, db2 = 0;
                    bool an = false, bn = false;
                    try { da = std::stod(va); an = true; } catch (...) {}
                    try { db2 = std::stod(vb); bn = true; } catch (...) {}
                    bool less;
                    if (an && bn) less = da < db2;
                    else less = va < vb;
                    return orderDesc ? !less && va != vb : less;
                });
        }

        // Build output: "ROW value1 value2...\n" per row, then "END\n"
        std::string out;
        for (const auto *rp : result) {
            out += "ROW " + formatRow(rp->values, colIdxs) + "\n";
        }
        out += "END\n";
        return out;
    }

    // =========================================================
    // INNER JOIN SELECT
    // =========================================================
    // FROM tableA INNER JOIN tableB ON tableA.col = tableB.col [WHERE ...] [ORDER BY ...]
    std::string t1Name = toUpper(trim(sql.substr(afterFrom, joinPos - afterFrom)));
    if (!t1Name.empty() && t1Name.back() == ';') t1Name.pop_back();
    t1Name = trim(t1Name);

    size_t afterJoin = joinPos + 12;
    size_t onPos = up.find(" ON ", afterJoin);
    if (onPos == std::string::npos) return "ERROR: missing ON clause\n";

    std::string t2Name = toUpper(trim(sql.substr(afterJoin, onPos - afterJoin)));
    t2Name = trim(t2Name);

    if (!db.tables.count(t1Name)) return "ERROR: table not found: " + t1Name + "\n";
    if (!db.tables.count(t2Name)) return "ERROR: table not found: " + t2Name + "\n";
    Table &tA = db.tables[t1Name];
    Table &tB = db.tables[t2Name];

    // Parse ON clause
    size_t afterOn = onPos + 4;
    size_t onEnd = std::string::npos;
    if (wherePos != std::string::npos && wherePos > onPos) onEnd = wherePos;
    if (orderPos != std::string::npos && (onEnd == std::string::npos || orderPos < onEnd)) onEnd = orderPos;

    std::string onClause = trim(onEnd == std::string::npos ? sql.substr(afterOn) : sql.substr(afterOn, onEnd - afterOn));
    if (!onClause.empty() && onClause.back() == ';') onClause.pop_back();
    onClause = trim(onClause);

    // ON t1.col = t2.col
    std::vector<std::string> onToks = tokenize(onClause);
    if (onToks.size() < 3) return "ERROR: invalid ON clause\n";
    std::string on1 = toUpper(onToks[0]);
    std::string on2 = toUpper(onToks[2]);

    int oi1 = findColIdx(tA, on1);
    int oi2 = findColIdx(tB, on2);
    if (oi1 == -1) return "ERROR: column not found in join: " + on1 + "\n";
    if (oi2 == -1) return "ERROR: column not found in join: " + on2 + "\n";

    // Build merged column list for column resolution
    // Combined: tA.cols... then tB.cols...
    // For column lookup in select/where/order, we need a merged "virtual table"
    struct MergedRow {
        std::vector<std::string> values;
    };

    // Resolve select columns against merged table
    // Column names can be TABLE.COL or just COL
    auto findMerged = [&](const std::string &colRef, int &outIdx) -> bool {
        std::string up2 = toUpper(colRef);
        size_t dot = up2.find('.');
        std::string tablePart = (dot != std::string::npos) ? up2.substr(0, dot) : "";
        std::string colPart   = (dot != std::string::npos) ? up2.substr(dot + 1) : up2;

        // Try tA
        for (int i = 0; i < (int)tA.columns.size(); i++) {
            std::string tc = toUpper(tA.columns[i]);
            if (tc == colPart || tc == up2) {
                if (tablePart.empty() || tablePart == t1Name) {
                    outIdx = i;
                    return true;
                }
            }
        }
        // Try tB
        for (int i = 0; i < (int)tB.columns.size(); i++) {
            std::string tc = toUpper(tB.columns[i]);
            if (tc == colPart || tc == up2) {
                if (tablePart.empty() || tablePart == t2Name) {
                    outIdx = (int)tA.columns.size() + i;
                    return true;
                }
            }
        }
        return false;
    };

    // Resolve select col indices
    std::vector<int> colIdxs;
    if (selColNames.size() == 1 && selColNames[0] == "*") {
        for (int i = 0; i < (int)tA.columns.size() + (int)tB.columns.size(); i++)
            colIdxs.push_back(i);
    } else {
        for (auto &cn : selColNames) {
            int idx = -1;
            if (!findMerged(cn, idx)) return "ERROR: column not found: " + cn + "\n";
            colIdxs.push_back(idx);
        }
    }

    // Parse WHERE
    bool hasWhere = (wherePos != std::string::npos);
    int wColIdx = -1;
    std::string wOp, wVal;
    if (hasWhere) {
        size_t wStart = wherePos + 7;
        size_t wEnd = (orderPos != std::string::npos && orderPos > wherePos) ? orderPos : std::string::npos;
        std::string wClause = trim(wEnd == std::string::npos ? sql.substr(wStart) : sql.substr(wStart, wEnd - wStart));
        if (!wClause.empty() && wClause.back() == ';') wClause.pop_back();
        wClause = trim(wClause);

        std::vector<std::string> wToks = tokenize(wClause);
        if (wToks.size() < 3) return "ERROR: incomplete WHERE clause\n";
        std::string wColName = toUpper(wToks[0]);
        wOp  = wToks[1];
        wVal = stripQuotes(wToks[2]);

        if (!findMerged(wColName, wColIdx))
            return "ERROR: column not found: " + wColName + "\n";
    }

    // Parse ORDER BY
    bool hasOrder = (orderPos != std::string::npos);
    int orderColIdx = -1;
    bool orderDesc = false;
    if (hasOrder) {
        size_t oStart = orderPos + 10;
        std::string oClause = trim(sql.substr(oStart));
        if (!oClause.empty() && oClause.back() == ';') oClause.pop_back();
        oClause = trim(oClause);
        std::vector<std::string> oToks = tokenize(oClause);
        if (!oToks.empty()) {
            std::string oc = toUpper(oToks[0]);
            if (!findMerged(oc, orderColIdx))
                return "ERROR: column not found in ORDER BY: " + oc + "\n";
            if (oToks.size() > 1 && toUpper(oToks[1]) == "DESC") orderDesc = true;
        }
    }

    // Perform join
    time_t now = time(nullptr);
    std::vector<std::vector<std::string>> joined;

    for (const auto &rA : tA.rows) {
        if (now > rA.expiry) continue;
        for (const auto &rB : tB.rows) {
            if (now > rB.expiry) continue;
            if (rA.values[oi1] == rB.values[oi2]) {
                std::vector<std::string> merged = rA.values;
                merged.insert(merged.end(), rB.values.begin(), rB.values.end());
                joined.push_back(std::move(merged));
            }
        }
    }

    // Filter WHERE
    if (hasWhere) {
        std::vector<std::vector<std::string>> filtered;
        for (auto &row : joined) {
            if (wColIdx < (int)row.size() &&
                compareValues(row[wColIdx], wVal, wOp))
                filtered.push_back(row);
        }
        joined = filtered;
    }

    // Sort
    if (hasOrder && orderColIdx >= 0) {
        std::stable_sort(joined.begin(), joined.end(),
            [&](const std::vector<std::string> &a, const std::vector<std::string> &b) {
                if (orderColIdx >= (int)a.size() || orderColIdx >= (int)b.size()) return false;
                const std::string &va = a[orderColIdx];
                const std::string &vb = b[orderColIdx];
                double da = 0, db2 = 0;
                bool an = false, bn = false;
                try { da = std::stod(va); an = true; } catch (...) {}
                try { db2 = std::stod(vb); bn = true; } catch (...) {}
                bool less;
                if (an && bn) less = da < db2;
                else less = va < vb;
                return orderDesc ? !less && va != vb : less;
            });
    }

    // Output
    std::string out;
    for (auto &row : joined) {
        std::string rowStr;
        for (size_t ci = 0; ci < colIdxs.size(); ci++) {
            if (ci > 0) rowStr += " ";
            if (colIdxs[ci] < (int)row.size())
                rowStr += row[colIdxs[ci]];
        }
        out += "ROW " + rowStr + "\n";
    }
    out += "END\n";
    return out;
}

//---------------------------------------------
// MAIN QUERY ROUTER
//---------------------------------------------
std::string executeQuery(Database &db, const std::string &rawSql) {
    std::string sql = trim(rawSql);
    if (!sql.empty() && sql.back() == ';') sql.pop_back();
    sql = trim(sql);
    if (sql.empty()) return "END\n";

    std::string up = toUpper(sql.substr(0, std::min(sql.size(), (size_t)20)));

    if (up.find("CREATE") == 0)
        return execCreate(db, sql) + "END\n";

    if (up.find("DROP") == 0)
        return execDrop(db, sql) + "END\n";

    if (up.find("DELETE") == 0)
        return execDelete(db, sql) + "END\n";

    if (up.find("INSERT") == 0)
        return execInsert(db, sql) + "END\n";


    if (up.find("SELECT") == 0) {
        std::string r = execSelect(db, sql);
        if (r.find("END\n") == std::string::npos)
            r += "END\n";
        return r;
    }


    return "ERROR: unknown command\nEND\n";
}
