#ifndef FLEXQL_H
#define FLEXQL_H

#include <cstdint>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct FlexQL FlexQL;

#define FLEXQL_OK 0
#define FLEXQL_ERROR 1

// Binary bulk insert row structure
typedef struct {
    const char **values;
    int col_count;
} BulkRow;

// Ultra-fast binary format: fixed-size field layout for direct memory ops
// Column: ID (DECIMAL=8), NAME (64 bytes), EMAIL (64 bytes), BALANCE (8), EXPIRES (8)
// Total: 152 bytes per row - NO PARSING NEEDED
#define BULK_ROW_SIZE 152
typedef struct {
    uint64_t id;
    char name[64];
    char email[64];
    uint64_t balance;
    uint64_t expires;
} BulkRowBinary;

int flexql_open(const char *host, int port, FlexQL **db);
int flexql_close(FlexQL *db);

int flexql_exec(
    FlexQL *db,
    const char *sql,
    int (*callback)(void*, int, char**, char**),
    void *arg,
    char **errmsg
);

// High-performance binary bulk insert (no SQL parsing overhead)
int flexql_bulk_insert_binary(
    FlexQL *db,
    const char *table_name,
    const BulkRow *rows,
    int row_count,
    char **errmsg
);

// Ultra-fast fixed-format binary insert (no parsing at all!)
int flexql_bulk_insert_fixed_binary(
    FlexQL *db,
    const char *table_name,
    const BulkRowBinary *rows,
    int row_count,
    char **errmsg
);

void flexql_free(void *ptr);

#ifdef __cplusplus
}
#endif

#endif