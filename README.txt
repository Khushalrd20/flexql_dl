g++ -O3 -march=native server.cpp database.cpp parser.cpp -o server -pthread


./server
g++ client.cpp -o client
./client


fuser -k 9000/tcp
make flexql-server
make benchmark_flexql
./flexql-server 
/benchmark_flexql




CREATE TABLE t1
CREATE TABLE t2

INSERT INTO t1 VALUES 1 Alice
INSERT INTO t1 VALUES 2 Bob
INSERT INTO t2 VALUES 1 90
INSERT INTO t2 VALUES 2 85

SELECT * FROM t1
SELECT * FROM t1 WHERE id = 1
SELECT * FROM t1 INNER JOIN t2 ON id

SELECT * FROM unknown
SELECT * FROM t1 INNER JOIN t2 ON id



 updated
 
CREATE TABLE t1 (ID, NAME);
CREATE TABLE t2 (ID, NAME);

INSERT INTO t1 VALUES (1, 'Khushu');
INSERT INTO t2 VALUES (1, 'alice');

SELECT * FROM t1;

SELECT * FROM t1 INNER JOIN t2 ON t1.ID = t2.ID;