import time
import random
import threading

from thrift import Thrift
from thrift.transport import TSocket, TTransport
from thrift.protocol import TCompactProtocol
from thrift.server import TServer
from thrift.transport.TTransport import TTransportException 
from thrift.Thrift import TException  # 导入 TException

from test import PingService  

import string


def generate_random_string(size_kb):

    size_bytes = size_kb * 1024  # Convert KB to Bytes
    return ''.join(random.choices(string.ascii_letters + string.digits, k=size_bytes))


class PingServiceHandler:
    def __init__(self):
        self = self      

    def ping(self, msg):  
        if msg == "kill":
            print("Received kill command. Shutting down...")
            self.running = False
            self.stop_event.set()  # 设置事件标志
            return "Server is shutting down"
        timestamp = int(time.time() * 1000)
        
        response = f"receive len: {len(msg)}"
        print(response)
        return response

    def output(self, msg):
        return f"You said: {msg}"

def start_server(port, handler):
    processor = PingService.Processor(handler)
    transport = TSocket.TServerSocket(host='localhost', port=port)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TCompactProtocol.TCompactProtocolFactory()

    server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)
    handler.server = server
    server.serve()
    print(f"Starting the Python server on port {port}...")

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
