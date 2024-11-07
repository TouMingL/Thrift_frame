# Thrift_frame
# 使用 Thrift 实现客户端与服务端通信

本文将从服务端与客户端实现入手，以python为主要例子，详细讲解使用 Apache Thrift 实现客户端与服务端通信，包括如何定义服务接口，如何实现断线自动重连等功能。并进一步深入 Thrift 协议的依赖关系，探讨在断线重连中的协议生命周期管理。

## 1. 服务端实现

### 1.1 定义 Thrift IDL 接口
使用 Thrift 需要首先定义 IDL（Interface Definition Language）接口文件，这里我们假设文件名为 `PingService.thrift`。定义接口的过程中，采用 `override` 方式在 Thrift IDL 中定义服务接口：
```thrift
service PingService {
    void ping(1:string msg),
    void output(1:string msg)
}
```
其中，ping 和 output 方法定义了基础的业务接口：

ping：接收客户端的消息并返回消息长度，用于标记吞吐不同数据量的数据所需要的时延。
output：返回客户端的消息，便于测试客户端到服务端的数据传输。

### 1.2 实现服务端业务逻辑
Thrift 会根据 IDL 生成对应的服务端接口代码。在服务端代码中，通过import自动生成的库文件实现实际业务逻辑：
```python
from thrift import Thrift
from thrift.transport import TSocket, TTransport
from thrift.protocol import TCompactProtocol
from thrift.server import TServer
from thrift.transport.TTransport import TTransportException 
from thrift.Thrift import TException

from test import PingService # 根据实际生产的gen-py结构来引用

import time


class PingServiceHandler:
    def __init__(self):
        self = self      

    def ping(self, msg):  
        timestamp = int(time.time() * 1000)
        response = f"receive len: {len(msg)}"
        print(response)

        return response

    def output(self, msg):
        return f"You said: {msg}"
```
ping：记录并返回消息长度，以便客户端验证消息传输的准确性。
output：返回客户端的消息内容，测试数据传输的完整性。
### 1.3 启动服务端并实例化协议
在服务端实现中，我们需要以下组件来启动并管理 Thrift 服务：
```python
def start_server(port, handler):
    processor = PingService.Processor(handler)
    transport = TSocket.TServerSocket(host='localhost', port=port)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TCompactProtocol.TCompactProtocolFactory()

    server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)
    handler.server = server
    server.serve()
    print(f"Starting the Python server on port {port}...")

def main():
    handler = PingServiceHandler()
    server_thread = threading.Thread(target=start_server, args=(9091, handler))
    server_thread.start()
```
TServerSocket：服务端监听的端口。
TBufferedTransport：用于封装传输层，提供数据缓冲，提高传输效率。
TCompactProtocol：服务端和客户端数据的压缩编码协议。
在服务端启动代码中，通过创建 TSimpleServer 实例化上述协议并启动服务监听。

## 2. 客户端实现
### 2.1 客户端功能及业务需求
为了保证客户端与服务端的完整通信，客户端主要包括以下功能：

send_ping：客户端向服务端发送不同大小的数据包，测试数据传输和服务端处理性能。
retry_transport：当连接断开或发生异常时，尝试重新连接，确保通信链路的稳定性。

为此我们先实现一个生成指定大小的随机字符串生成函数
```python
import string
import random
def generate_random_string(size_kb):

    size_bytes = size_kb * 1024  # Convert KB to Bytes
    return ''.join(random.choices(string.ascii_letters + string.digits, k=size_bytes))

def main():
    handler = PingServiceHandler()
    
    str_128k = generate_random_string(128)
    str_256k = generate_random_string(256)
    str_512k = generate_random_string(512)
    str_1024k = generate_random_string(1024)
```
### 2.2 客户端所需的协议及组件
客户端与服务端的通信依赖以下协议和组件的正确配置：

    TSocket：           客户端与服务端的 TCP 连接。
    TBufferedTransport：封装数据传输，减少直接网络传输带来的延迟。
    TCompactProtocol：  数据压缩协议，与服务端保持一致的编码方式。
    PingServiceClient： 客户端接口，用于发送实际的 RPC 请求。

### 2.3 客户端的协议生命周期管理
##### 2.3.1 使用类封装客户端
在客户端中，协议栈的生命周期管理至关重要，因为 Thrift 的通信链路分层设计，TSocket、TTransport 和 TProtocol 组件是相互依赖的。
我们使用封装的方式来保证不同功能之间协议栈的生命周期一致性。

```python
class ThriftClient:
    def __init__(self, port):
        self.port = port
        self.socket = None
        self.transport = None
        self.protocol = None
        self.client = None
        self.init_client()

    def init_client(self):
        try:
            self.socket = TSocket.TSocket('localhost', self.port)
            self.transport = TTransport.TBufferedTransport(self.socket)
            self.protocol = TCompactProtocol.TCompactProtocol(self.transport)
            self.client = PingService.Client(self.protocol)
            
        except TTransportException as tx:
            if "NOT_OPEN" in str(tx):
                print(f"ERROR: Connection refused. Unable to open transport on {self.port}.")
                time.sleep(1)
            else :
                print(f"Transport Exception: {tx}")
        except TException as tx:
            print(f"Thrift Exception: {tx}")
        except Exception as e:
            print(f"Unexpected error: {e}")

    def send_ping(self, msg):
        try:
            if self.transport.isOpen() == False:
                self.transport.close()
                print("Transport is not open, try reconnect")
                self.reconnect()
                self.retry_transport()
            
            try:
                start_time = time.time()
                ping_response = self.client.ping(msg)  
                end_time = time.time()

                duration = (end_time - start_time) * 1e9  # 转换为纳秒
                print(f"Ping Response:{ping_response}")
                print(f"Ping to {self.port} Call Duration: {duration} ns")
            except TException as tx:
                print(f"PING ERROR: {tx}")
                print("BROKEN PIPE DETECTED, TRYING TO RECONNECT NOW")
                self.reconnect()
            
        except TTransportException as tx:
            print(f"Transport Exception: {tx}")
        except TException as tx:
            print(f"Thrift Exception: {tx}")
        except Exception as e:
            print(f"Unexpected error: {e}")
```
##### 2.3.2 断线重连机制的实现
我们前面描述了Thrift组件的相互依赖关系，那么如果最底层的套接字连接断开，也就是服务端掉线时应该怎么处理？

直接调用 `transport->open` 来重建连接必然是不可行的。原因在于：

1. **协议链路的依赖关系**：在 Thrift 中，`TSocket`、`TTransport` 和 `TProtocol` 层级相互依赖，状态需要保持一致。底层 `TSocket` 断开会导致 `TTransport` 层的缓冲和 `TProtocol` 层的编码数据都处于失效状态，仅重启 `TSocket` 或 `TTransport` 并不能同步恢复整个协议链路的正常状态。

2. **缓冲数据清理不彻底**：当 `TTransport` 层断开后，可能残留未发送的缓冲数据。重新调用 `transport->open` 不能清除这些数据，从而导致传输数据不完整或出现数据错误。完整的协议重置可以确保清空缓存，避免传输过程中产生错误。

3. **内存和资源一致性**：在调用 `transport->open` 时，如果协议栈未被彻底释放，可能会导致协议内部资源不一致，甚至出现内存泄漏。重新初始化整个协议栈确保资源状态从零开始，避免潜在的内存泄漏。

因此，在 `ping` 方法抛出 `broken pipe` 时，最合适的做法是彻底释放 `TSocket`、`TTransport` 和 `TProtocol`，重新初始化协议栈，确保重连后的通信链路处于干净、完整的状态。

在客户端的 reconnect 方法中，我们需要清理和释放所有协议组件，包括 TSocket、TTransport 和 TProtocol，确保在重连时无残留状态：
```python
class ThriftClient:# 同样的代码不再赘述
    def reconnect(self):
        self.transport = None
        self.socket = None
        self.protocol = None
        self.client = None
        self.init_client()

    def retry_transport(self):
        while(True):
            try:
                self.transport.open()
                print("Connected to %d", self.port)
                break
            except TTransportException as tx:
                if "NOT_OPEN" in str(tx):
                    print(f"ERROR: Connection refused. Unable to open transport on {self.port}.")
                    time.sleep(1)
                else :
                    print(f" retry_transport status: {tx}")

    def close_connection(self):
        if self.transport:
            self.transport.close()
            print(f"Connection to {self.port} closed")
```
并在reconnect的最后，重新调用 init_client 方法，创建新的协议实例，恢复到初始的连接状态。
## 3. 代码调用顺序概览
```python
import threading
/*
把客户端和服务端函数复制进来就可以了
*/
def main():
    handler = PingServiceHandler()
    
    str_128k = generate_random_string(128)
    str_256k = generate_random_string(256)
    str_512k = generate_random_string(512)
    str_1024k = generate_random_string(1024)

    server_thread = threading.Thread(target=start_server, args=(9091, handler))
    server_thread.start()
    time.sleep(1)

    client_9090 = ThriftClient(port=9090)
    print("Connected to handler1(c++) ")
    client_9092 = ThriftClient(port=9092)
    print("Connected to handler1(golang) ")

    while True:
        print("(c++) ")
        client_9090.send_ping(str_128k)
        client_9090.send_ping(str_256k)
        client_9090.send_ping(str_512k)
        client_9090.send_ping(str_1024k)

        print("(golang) ")
        client_9092.send_ping(str_128k)
        client_9092.send_ping(str_256k)
        client_9092.send_ping(str_512k)
        client_9092.send_ping(str_1024k)
        time.sleep(1)  # 设定一些延迟以避免过快发送



if __name__ == "__main__":
    main()

```
## 4. 总结
以上就是一个 Thrift 实现一个客户端-服务端架构的应用程序，主要功能包括，不同数据量的时延测试以及客户端断线重连机制。

