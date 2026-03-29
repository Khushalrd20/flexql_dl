CXX = g++
CXXFLAGS = -std=c++17 -O2 -Wall -pthread

all: flexql-server flexql-client benchmark_flexql

# Server binary
flexql-server: server.cpp database.cpp parser.h database.h
	$(CXX) $(CXXFLAGS) -o flexql-server server.cpp database.cpp

# Interactive client binary
flexql-client: client.cpp
	$(CXX) $(CXXFLAGS) -o flexql-client client.cpp

# Shared library for the C API (used by benchmark)
libflexql.so: flexql_api.cpp flexql.h
	$(CXX) $(CXXFLAGS) -shared -fPIC -o libflexql.so flexql_api.cpp

# Static library (alternative)
libflexql.a: flexql_api.cpp flexql.h
	$(CXX) $(CXXFLAGS) -c -o flexql_api.o flexql_api.cpp
	ar rcs libflexql.a flexql_api.o

# Benchmark — links against the API library
benchmark_flexql: benchmark_flexql.cpp libflexql.a flexql.h
	$(CXX) $(CXXFLAGS) -o benchmark_flexql benchmark_flexql.cpp -L. -lflexql

clean:
	rm -f flexql-server flexql-client benchmark_flexql libflexql.so libflexql.a flexql_api.o

.PHONY: all clean
