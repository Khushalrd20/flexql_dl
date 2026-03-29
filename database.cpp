#include "database.h"
#include <sstream>
#include <fstream>
#include <iostream>

//---------------------------------------------
// LRU CACHE
//---------------------------------------------
void LRUCache::put(std::string key, std::string val)
{
    if(mp.find(key)!=mp.end())
        dq.erase(mp[key]);

    dq.push_front({key,val});
    mp[key]=dq.begin();

    if(dq.size()>capacity)
    {
        auto last=dq.back();
        mp.erase(last.first);
        dq.pop_back();
    }
}

std::string LRUCache::get(std::string key)
{
    if(mp.find(key)==mp.end())
        return "";

    auto it=mp[key];
    std::string val=it->second;

    dq.erase(it);
    dq.push_front({key,val});
    mp[key]=dq.begin();

    return val;
}

//---------------------------------------------
// CREATE TABLE
//---------------------------------------------
std::string Database::createTable(std::string name,std::vector<std::string> cols)
{
    Table t;
    t.name=name;
    t.columns=cols;

    tables[name]=t;
    return "Table created\n";
}

//---------------------------------------------
// BULK INSERT (FAST)
//---------------------------------------------
void Database::bulkInsert(std::string name,
    const std::vector<std::pair<std::string, std::string>>& batch)
{
    Table &t = tables[name];

    t.rows.reserve(t.rows.size() + batch.size());

    for(const auto &p : batch)
    {
        if(t.primaryIndex.find(p.first) != t.primaryIndex.end())
            continue;

        Row r;
        r.expiry = time(NULL) + 3600;

        r.values.push_back(p.first);
        r.values.push_back(p.second);

        t.primaryIndex[p.first] = t.rows.size();
        t.rows.emplace_back(std::move(r));
    }
}

//---------------------------------------------
// INSERT ROW
//---------------------------------------------
std::string Database::insertRow(std::string name, std::vector<std::string> values)
{
    if(tables.find(name)==tables.end())
        return "Table not found\n";

    Table &t=tables[name];

    if(values.size()!=t.columns.size())
        return "Column count mismatch\n";

    std::string pk = values[0];

    if(t.primaryIndex.find(pk)!=t.primaryIndex.end())
        return "Duplicate primary key\n";

    Row r;
    r.expiry = time(NULL)+3600;
    r.values = std::move(values);

    t.primaryIndex[pk] = t.rows.size();
    t.rows.emplace_back(std::move(r));

    return "Row inserted\n";
}

//---------------------------------------------
// SELECT ALL
//---------------------------------------------
std::string Database::selectAll(std::string name)
{
    if(tables.find(name)==tables.end())
        return "Table not found\n";

    Table &t=tables[name];
    std::stringstream ss;

    for(auto &c:t.columns)
        ss<<c<<" ";
    ss<<"\n";

    for(auto &row:t.rows)
    {
        if(time(NULL)>row.expiry)
            continue;

        for(auto &v:row.values)
            ss<<v<<" ";
        ss<<"\n";
    }

    return ss.str();
}

//---------------------------------------------
// SELECT WHERE
//---------------------------------------------
std::string Database::selectWhere(std::string name,std::string col,std::string val)
{
    if(tables.find(name)==tables.end())
        return "Table not found\n";

    Table &t=tables[name];

    int idx=-1;
    for(int i=0;i<t.columns.size();i++)
        if(t.columns[i]==col) idx=i;

    if(idx==-1) return "Column not found\n";

    std::stringstream ss;

    for(auto &row:t.rows)
    {
        if(row.values[idx]==val)
        {
            for(auto &v:row.values)
                ss<<v<<" ";
            ss<<"\n";
        }
    }

    return ss.str();
}

//---------------------------------------------
// JOIN
//---------------------------------------------
std::string Database::joinTables(std::string t1,std::string t2,std::string col)
{
    if(tables.find(t1)==tables.end()) return "Table not found\n";
    if(tables.find(t2)==tables.end()) return "Table not found\n";

    Table &a=tables[t1];
    Table &b=tables[t2];

    int i1=-1,i2=-1;

    for(int i=0;i<a.columns.size();i++)
        if(a.columns[i]==col) i1=i;

    for(int i=0;i<b.columns.size();i++)
        if(b.columns[i]==col) i2=i;

    if(i1==-1 || i2==-1) return "Column not found\n";

    std::stringstream ss;

    for(auto &r1:a.rows)
    {
        for(auto &r2:b.rows)
        {
            if(r1.values[i1]==r2.values[i2])
            {
                for(auto &v:r1.values) ss<<v<<" ";
            }
        }
    }

    return ss.str();
}

//---------------------------------------------
// 🗄️ PERSISTENCE: SAVE TO DISK
//---------------------------------------------
bool Database::saveToDisk(const std::string& filename)
{
    try {
        // Ensure directory exists
        std::string dir = filename.substr(0, filename.find_last_of("/"));
        if (!dir.empty() && dir != filename) {
            std::string mkdir_cmd = "mkdir -p " + dir;
            system(mkdir_cmd.c_str());
        }

        std::ofstream file(filename, std::ios::binary);
        if (!file.is_open()) {
            std::cerr << "Error: Cannot open file for writing: " << filename << std::endl;
            return false;
        }

        // Write number of tables
        size_t numTables = tables.size();
        file.write(reinterpret_cast<const char*>(&numTables), sizeof(numTables));

        // Write each table
        for (const auto& tablePair : tables) {
            const std::string& tableName = tablePair.first;
            const Table& table = tablePair.second;

            // Write table name length and name
            size_t nameLen = tableName.size();
            file.write(reinterpret_cast<const char*>(&nameLen), sizeof(nameLen));
            file.write(tableName.c_str(), nameLen);

            // Write columns
            size_t numCols = table.columns.size();
            file.write(reinterpret_cast<const char*>(&numCols), sizeof(numCols));
            for (const std::string& col : table.columns) {
                size_t colLen = col.size();
                file.write(reinterpret_cast<const char*>(&colLen), sizeof(colLen));
                file.write(col.c_str(), colLen);
            }

            // Write rows
            size_t numRows = table.rows.size();
            file.write(reinterpret_cast<const char*>(&numRows), sizeof(numRows));
            for (const Row& row : table.rows) {
                // Write expiry
                file.write(reinterpret_cast<const char*>(&row.expiry), sizeof(row.expiry));

                // Write values
                size_t numValues = row.values.size();
                file.write(reinterpret_cast<const char*>(&numValues), sizeof(numValues));
                for (const std::string& val : row.values) {
                    size_t valLen = val.size();
                    file.write(reinterpret_cast<const char*>(&valLen), sizeof(valLen));
                    file.write(val.c_str(), valLen);
                }
            }

            // Write primary indexes (string keys)
            size_t numStringKeys = table.primaryIndex.size();
            file.write(reinterpret_cast<const char*>(&numStringKeys), sizeof(numStringKeys));
            for (const auto& indexPair : table.primaryIndex) {
                size_t keyLen = indexPair.first.size();
                file.write(reinterpret_cast<const char*>(&keyLen), sizeof(keyLen));
                file.write(indexPair.first.c_str(), keyLen);
                file.write(reinterpret_cast<const char*>(&indexPair.second), sizeof(indexPair.second));
            }

            // Write primary indexes (64-bit keys)
            size_t numUint64Keys = table.primaryIndex64.size();
            file.write(reinterpret_cast<const char*>(&numUint64Keys), sizeof(numUint64Keys));
            for (const auto& indexPair : table.primaryIndex64) {
                file.write(reinterpret_cast<const char*>(&indexPair.first), sizeof(indexPair.first));
                file.write(reinterpret_cast<const char*>(&indexPair.second), sizeof(indexPair.second));
            }
        }

        file.close();
        std::cout << "Database saved to " << filename << " (" << tables.size() << " tables)" << std::endl;
        return true;
    }
    catch (const std::exception& e) {
        std::cerr << "Error saving database: " << e.what() << std::endl;
        return false;
    }
}

//---------------------------------------------
// 🗄️ PERSISTENCE: LOAD FROM DISK
//---------------------------------------------
bool Database::loadFromDisk(const std::string& filename)
{
    try {
        std::ifstream file(filename, std::ios::binary);
        if (!file.is_open()) {
            std::cout << "No existing database file found: " << filename << " (starting fresh)" << std::endl;
            return true; // Not an error - just no existing data
        }

        // Clear existing data
        tables.clear();

        // Read number of tables
        size_t numTables;
        file.read(reinterpret_cast<char*>(&numTables), sizeof(numTables));

        // Read each table
        for (size_t t = 0; t < numTables; ++t) {
            Table table;

            // Read table name
            size_t nameLen;
            file.read(reinterpret_cast<char*>(&nameLen), sizeof(nameLen));
            std::string tableName(nameLen, '\0');
            file.read(&tableName[0], nameLen);
            table.name = tableName;

            // Read columns
            size_t numCols;
            file.read(reinterpret_cast<char*>(&numCols), sizeof(numCols));
            table.columns.resize(numCols);
            for (size_t c = 0; c < numCols; ++c) {
                size_t colLen;
                file.read(reinterpret_cast<char*>(&colLen), sizeof(colLen));
                table.columns[c].resize(colLen);
                file.read(&table.columns[c][0], colLen);
            }

            // Read rows
            size_t numRows;
            file.read(reinterpret_cast<char*>(&numRows), sizeof(numRows));
            table.rows.resize(numRows);
            for (size_t r = 0; r < numRows; ++r) {
                Row& row = table.rows[r];

                // Read expiry
                file.read(reinterpret_cast<char*>(&row.expiry), sizeof(row.expiry));

                // Read values
                size_t numValues;
                file.read(reinterpret_cast<char*>(&numValues), sizeof(numValues));
                row.values.resize(numValues);
                for (size_t v = 0; v < numValues; ++v) {
                    size_t valLen;
                    file.read(reinterpret_cast<char*>(&valLen), sizeof(valLen));
                    row.values[v].resize(valLen);
                    file.read(&row.values[v][0], valLen);
                }
            }

            // Read primary indexes (string keys)
            size_t numStringKeys;
            file.read(reinterpret_cast<char*>(&numStringKeys), sizeof(numStringKeys));
            for (size_t k = 0; k < numStringKeys; ++k) {
                size_t keyLen;
                file.read(reinterpret_cast<char*>(&keyLen), sizeof(keyLen));
                std::string key(keyLen, '\0');
                file.read(&key[0], keyLen);
                int value;
                file.read(reinterpret_cast<char*>(&value), sizeof(value));
                table.primaryIndex[key] = value;
            }

            // Read primary indexes (64-bit keys)
            size_t numUint64Keys;
            file.read(reinterpret_cast<char*>(&numUint64Keys), sizeof(numUint64Keys));
            for (size_t k = 0; k < numUint64Keys; ++k) {
                uint64_t key;
                file.read(reinterpret_cast<char*>(&key), sizeof(key));
                int value;
                file.read(reinterpret_cast<char*>(&value), sizeof(value));
                table.primaryIndex64[key] = value;
            }

            tables[tableName] = std::move(table);
        }

        file.close();
        std::cout << "Database loaded from " << filename << " (" << tables.size() << " tables)" << std::endl;
        return true;
    }
    catch (const std::exception& e) {
        std::cerr << "Error loading database: " << e.what() << std::endl;
        return false;
    }
}