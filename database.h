#ifndef DATABASE_H
#define DATABASE_H

#include <vector>
#include <map>
#include <string>
#include <unordered_map>
#include <ctime>
#include <list>
#include <utility>
#include <cstdint>

//---------------------------------------------
// ROW
//---------------------------------------------
struct Row
{
    std::vector<std::string> values;
    time_t expiry;
};

//---------------------------------------------
// TABLE (OPTIMIZED FOR BULK INSERT)
//---------------------------------------------
struct Table
{
    std::string name;
    std::vector<std::string> columns;
    std::vector<Row> rows;

    // Direct 64-bit primary key index (NO STRING CONVERSIONS!)
    std::unordered_map<uint64_t, int> primaryIndex64;
    std::unordered_map<std::string, int> primaryIndex;
};

//---------------------------------------------
// LRU CACHE
//---------------------------------------------
class LRUCache
{
    int capacity;
    std::list<std::pair<std::string,std::string>> dq;
    std::unordered_map<std::string,
        std::list<std::pair<std::string,std::string>>::iterator> mp;

public:
    LRUCache(int c=50){capacity=c;}

    void put(std::string key,std::string val);
    std::string get(std::string key);
};

//---------------------------------------------
// DATABASE
//---------------------------------------------
class Database
{
public:

    std::map<std::string,Table> tables;
    LRUCache cache;

    Database():cache(50){}

    std::string createTable(std::string name,std::vector<std::string> cols);
    std::string insertRow(std::string name,std::vector<std::string> values);

    // 🔥 BULK INSERT
    void bulkInsert(std::string name,
        const std::vector<std::pair<std::string, std::string>>& batch);

    std::string selectAll(std::string name);
    std::string selectWhere(std::string name,std::string col,std::string val);
    std::string joinTables(std::string t1,std::string t2,std::string col);

    // 🗄️ PERSISTENCE METHODS
    bool saveToDisk(const std::string& filename = "flexql_data.db");
    bool loadFromDisk(const std::string& filename = "flexql_data.db");
};

#endif  