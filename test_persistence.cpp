#include "database.h"
#include <iostream>
#include <vector>

int main() {
    Database db;

    // Create a test table
    std::vector<std::string> cols = {"id", "name", "age"};
    std::string result = db.createTable("users", cols);
    std::cout << "Create table: " << result << std::endl;

    // Insert some test data
    std::vector<std::string> row1 = {"1", "Alice", "25"};
    std::vector<std::string> row2 = {"2", "Bob", "30"};

    result = db.insertRow("users", row1);
    std::cout << "Insert row1: " << result << std::endl;

    result = db.insertRow("users", row2);
    std::cout << "Insert row2: " << result << std::endl;

    // Select all to verify data
    result = db.selectAll("users");
    std::cout << "Select all:\n" << result << std::endl;

    // Save to disk
    if (db.saveToDisk("test_persistence.db")) {
        std::cout << "✅ Database saved successfully!" << std::endl;
    } else {
        std::cout << "❌ Failed to save database!" << std::endl;
        return 1;
    }

    // Clear the database
    db.tables.clear();
    std::cout << "Database cleared. Tables: " << db.tables.size() << std::endl;

    // Load from disk
    if (db.loadFromDisk("test_persistence.db")) {
        std::cout << "✅ Database loaded successfully!" << std::endl;
    } else {
        std::cout << "❌ Failed to load database!" << std::endl;
        return 1;
    }

    // Verify data was loaded
    result = db.selectAll("users");
    std::cout << "Select all after loading:\n" << result << std::endl;

    return 0;
}