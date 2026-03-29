#include <iostream>
#include <cstring>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <chrono>

#define PORT 9000
#define BUFFER_SIZE 4096

//---------------------------------------------
// FAST NUMBER PARSER
//---------------------------------------------
long fastExtract(const std::string& s)
{
    long num = 0;
    bool found = false;

    for(char c : s)
    {
        if(c >= '0' && c <= '9')
        {
            found = true;
            num = num * 10 + (c - '0');
        }
        else if(found)
            break;
    }

    return num;
}

//---------------------------------------------
// MAIN CLIENT
//---------------------------------------------
int main()
{
    while(true)
    {
        int sock = socket(AF_INET, SOCK_STREAM, 0);
        if(sock < 0)
        {
            std::cout << "Socket error\n";
            continue;
        }

        struct sockaddr_in serv_addr{};
        serv_addr.sin_family = AF_INET;
        serv_addr.sin_port = htons(PORT);

        if(inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr) <= 0)
        {
            std::cout << "Invalid address\n";
            close(sock);
            continue;
        }

        if(connect(sock, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0)
        {
            std::cout << "Connection Failed\n";
            close(sock);
            continue;
        }

        std::string query;
        std::cout << "FlexQL> ";
        std::getline(std::cin, query);

        if(query == "exit")
        {
            close(sock);
            break;
        }

        query += "\n";

        //-----------------------------------------
        // CLIENT-SIDE TIMING START
        //-----------------------------------------
        auto start = std::chrono::high_resolution_clock::now();

        send(sock, query.c_str(), query.size(), 0);

        //-----------------------------------------
        // SAFE RECEIVE (NO HANG FIX)
        //-----------------------------------------
        char buffer[BUFFER_SIZE];
        std::string response;
        response.reserve(8192);

        while(true)
        {
            int bytes = recv(sock, buffer, BUFFER_SIZE, 0);

            if(bytes <= 0)
                break;

            response.append(buffer, bytes);

            // CRITICAL FIX → stop early
            if(bytes < BUFFER_SIZE)
                break;
        }

        //-----------------------------------------
        //  CLIENT-SIDE TIMING END
        //-----------------------------------------
        auto stop = std::chrono::high_resolution_clock::now();
        long client_time = std::chrono::duration_cast<std::chrono::milliseconds>(stop - start).count();

        //-----------------------------------------
        // SMART OUTPUT
        //-----------------------------------------
        if(response.find("Inserted") != std::string::npos)
        {
            long rows = fastExtract(response);
            long server_time = fastExtract(response.substr(response.find("in")));

            double server_tp = (server_time > 0) ? (rows * 1000.0 / server_time) : 0;
            double client_tp = (client_time > 0) ? (rows * 1000.0 / client_time) : 0;

            std::cout << "\n========== 🚀 FLEXQL INSERT STATS ==========\n";
            std::cout << "Rows Inserted        : " << rows << "\n";
            std::cout << "Server Time          : " << server_time << " ms\n";
            std::cout << "Client Time          : " << client_time << " ms\n";
            std::cout << "Server Throughput    : " << server_tp << " rows/sec\n";
            std::cout << "Client Throughput    : " << client_tp << " rows/sec\n";
            std::cout << "===========================================\n\n";
        }
        else if(response.find("SELECT executed") != std::string::npos)
        {
            long server_time = fastExtract(response);

            std::cout << "\n========== ⚡ QUERY PERFORMANCE ==========\n";
            std::cout << "Server Time          : " << server_time << " ms\n";
            std::cout << "Client Time          : " << client_time << " ms\n";
            std::cout << "=========================================\n\n";
        }
        else
        {
            std::cout << response << std::endl;
        }

        close(sock);
    }

    return 0;
}