#include "PingService.h"
#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/server/TSimpleServer.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransport.h>
#include <thrift/transport/TTransportUtils.h>
#include <thrift/transport/TTransportException.h>
#include <iostream>
#include <sstream>
#include <string>
#include <cstdlib>
#include <ctime>
#include <memory>
#include <chrono>
#include <thread>

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;

std::atomic<bool> serverRunning(true);
std::string generate_random_string(size_t size_kb)
{
    const std::string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    size_t size_bytes = size_kb * 1024; // Convert KB to Bytes
    std::string result;
    result.reserve(size_bytes);

    srand((unsigned)time(0));
    for (size_t i = 0; i < size_bytes; ++i)
    {
        result += chars[rand() % chars.size()];
    }

    return result;
}

class PingServiceHandler : public PingServiceIf // C++通过继承而非引用的方式来实现业务功能
{
    public:
        PingServiceHandler() = default;

        void ping(std::string &_return, const std::string &msg) override
        {
            try{
                std::ostringstream oss;
                oss << "receive len: " << msg.size();
                _return = oss.str();
                printf("%s\n", _return.c_str());
            }
            catch (const std::exception &e){
                std::cerr << "Error in ping method: " << e.what() << std::endl;
                _return = "Error occurred";
            }
        }

        void output(std::string &_return, const std::string &msg) override
        {
            try
            {
                _return = "You said: " + msg;
                std::cout << _return << std::endl;
            }
            catch (const std::exception &e)
            {
                std::cerr << "Error in output method: " << e.what() << std::endl;
                _return = "Error occurred";
            }
        }
};

// 启动服务端
void startServer()
{
    while (serverRunning)
    {
        int port = 9090;
        std::shared_ptr<PingServiceHandler> handler(new PingServiceHandler());
        std::shared_ptr<TProcessor> processor(new PingServiceProcessor(handler));
        std::shared_ptr<TServerTransport> serverTransport(new TServerSocket(port));
        std::shared_ptr<TTransportFactory> transportFactory(new TBufferedTransportFactory());
        std::shared_ptr<TProtocolFactory> protocolFactory(new TCompactProtocolFactory());

        TSimpleServer server(processor, serverTransport, transportFactory, protocolFactory);
        printf("Starting the server on port %d...\n", port);
        server.serve();
    }
}


class ClientHandler{
    
    public:
        ClientHandler(int p) : port(p) {
            init_client();
            
        }   

        ~ClientHandler() {
            if (transport && transport->isOpen()) {
            transport->close();
            }
        }

        void send_ping(const std::string& str) {
            try{
                if (transport->isOpen() == 0) {
                    transport->close();
                    std::cerr << "Transport is not open, try reconnect" << std::endl;
                    reconnect();
                    retry_transport();
                } 

                std::string pingResponse;
                try {
                    auto start = std::chrono::high_resolution_clock::now();
                    client->ping(pingResponse, str);
                    auto end = std::chrono::high_resolution_clock::now();
                    auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
                    printf("Response from server on port %d: %zu\n", port, pingResponse.length());
                    printf("Ping %d Call Duration: %lld ns\n", port, duration);
                }
                catch (const TException &tx){
                    std::cerr << "PING ERROR: " << tx.what() << std::endl;
                    std::cerr << "BROKEN PIPE DETECTED, TRYING TO RECONNECT NOW" << std::endl;
                    reconnect();
                }
            } catch (const TTransportException &tx){
                if (tx.getType() == TTransportException::NOT_OPEN){
                    std::cerr << "ERROR: Connection refused. Unable to open "  << port << " transport." << std::endl;
                } else{
                    std::cerr << " send_ping status: " << tx.getType() << "\nTTransportException ERROR: " << tx.what() << std::endl;
                }
            } catch (const TException &tx){
                
                std::cerr << " TException ERROR: " << tx.what() << std::endl;
            }
        }

        void retry_transport() {
            while (true) {
                try {
                    transport->open(); 
                    std::cout << "Connected to " << port << std::endl;
                    break; 
                } catch (const TTransportException &tx){
                    if (tx.getType() == TTransportException::NOT_OPEN){
                        std::cerr << "ERROR: Connection refused. Unable to open "  << port << " transport." << std::endl;
                        std::this_thread::sleep_for(std::chrono::seconds(1));
                    } else{
                        std::cerr << " retry_transport status: " << tx.getType() << "\nTTransportException ERROR: " << tx.what() << std::endl;
                    } 
                }
            }
        }
        
        void reconnect() {
            transport.reset();
            socket.reset();
            protocol.reset();
            client.reset();

            init_client();
        }
    private:
        int port;
        std::shared_ptr<TSocket>            socket;
        std::shared_ptr<TTransport>         transport;
        std::shared_ptr<TProtocol>          protocol;
        std::shared_ptr<PingServiceClient>  client;

        void init_client() {
            try {
                socket      = std::make_shared<TSocket>("localhost", port);
                transport   = std::make_shared<TBufferedTransport>(socket);
                protocol    = std::make_shared<TCompactProtocol>(transport);
                client      = std::make_shared<PingServiceClient>(protocol);
            } 
            catch (const TTransportException &tx) {
                if (tx.getType() == TTransportException::NOT_OPEN){
                    std::cerr << "ERROR: Connection refused. Unable to open transport on "  << port << std::endl;
                    std::this_thread::sleep_for(std::chrono::seconds(1));
                } else{
                    std::cerr << " retry_transport status: " << tx.getType() << "\nTTransportException ERROR: " << tx.what() << std::endl;
                } 
            } catch (const TException &tx) {
                std::cerr << " TException ERROR: " << tx.what() << std::endl;
            }
        }
        
        
};

int main()
{
    std::string str_128k  = generate_random_string(128);
    std::string str_256k  = generate_random_string(256);
    std::string str_512k  = generate_random_string(512);
    std::string str_1024k = generate_random_string(1024);

    
    std::thread serverThread(startServer);
    std::this_thread::sleep_for(std::chrono::seconds(1));

    ClientHandler handler1(9091);
    std::cout << "Connected to handler1(python) "<< std::endl;
    ClientHandler handler2(9092);
    std::cout << "Connected to handler2(golang) "<< std::endl;

    std::thread t1([&handler1]() {
        handler1.retry_transport();
    });

    std::thread t2([&handler2]() {
        handler2.retry_transport();
    });

    while (true)
    {
        std::cout << "(python) "<< std::endl;
        handler1.send_ping(str_128k);
        handler1.send_ping(str_256k);
        handler1.send_ping(str_512k);
        handler1.send_ping(str_1024k);

        std::cout << "(golang) "<< std::endl;
        handler2.send_ping(str_128k);
        handler2.send_ping(str_256k);
        handler2.send_ping(str_512k);
        handler2.send_ping(str_1024k);
     
        std::this_thread::sleep_for(std::chrono::seconds(1)); // 避免刷屏
    }

    // 清理
    serverThread.join();
    return 0;
}
