package main

import (
    "context"
    "fmt"
    "log"
    "math/rand"
    "sync"
    "time"

    "github.com/apache/thrift/lib/go/thrift"
    "kp_thrift/test" //kp_tghrft替换为go init的project名
)

const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"

func generate_random_string(size_kb int) string {
    sizeBytes := size_kb * 1024
    result := make([]byte, sizeBytes)

    rand.Seed(time.Now().UnixNano())
    for i := range result {
        result[i] = chars[rand.Intn(len(chars))]
    }

    return string(result)
}

type PingServiceHandler struct {
    server *thrift.TSimpleServer // 服务器的引用
    wg     *sync.WaitGroup
}

func (h *PingServiceHandler) Ping(ctx context.Context, msg string) (string, error) {
    if msg == "kill" {
        go h.shutdown()
        return "Server is shutting down", nil
    }
    
    response := fmt.Sprintf("receive len: %d", len(msg))
    return response, nil
}

func (h *PingServiceHandler) Output(ctx context.Context, msg string) (string, error) {
    response := fmt.Sprintf("You said: %s", msg)
    return response, nil
}

func (h *PingServiceHandler) shutdown() {
    h.server.Stop()
    time.Sleep(1 * time.Second) // 等待1秒钟，确保端口释放
    h.wg.Done()
}

func startServer(port string, wg *sync.WaitGroup) *thrift.TSimpleServer {
    defer wg.Add(1)

    handler := &PingServiceHandler{wg: wg}
    processor := test.NewPingServiceProcessor(handler)

    serverTransport, err := thrift.NewTServerSocket("localhost:" + port)
    if err != nil {
        log.Fatalf("Error creating server socket: %v", err)
    }

    transportFactory := thrift.NewTBufferedTransportFactory(8192)
    protocolFactory := thrift.NewTCompactProtocolFactory()

    server := thrift.NewTSimpleServer4(processor, serverTransport, transportFactory, protocolFactory)
    handler.server = server

    go func() {
        if err := server.Serve(); err != nil {
            log.Fatalf("Error starting server: %v", err)
        }
    }()
    fmt.Printf("Server start@%s\n", port)
    return server
}

type ThriftClient struct { //这里使用结构体来保持生命周期的一致性
	port      int

	socket    *thrift.TSocket
	transport thrift.TTransport
	protocol  thrift.TProtocol
	client    *test.PingServiceClient

	mu        sync.Mutex
}


func NewThriftClient(p int) (*ThriftClient, error) {
	client_handler := &ThriftClient{ port: p }
	if err := client_handler.init_client(); err != nil {
        return nil, err
    }

	return client_handler, nil
}

func (tc *ThriftClient) init_client() error {
    var err error

    tc.socket, err = thrift.NewTSocket(fmt.Sprintf("localhost:%d", tc.port))
    if err != nil {
        fmt.Printf("failed to create socket: %v", err)
    }

    transportFactory := thrift.NewTBufferedTransportFactory(8192)
    tc.transport, err = transportFactory.GetTransport(tc.socket)
    if err != nil {
        fmt.Printf("Failed to create transport: %v\n", err)
    }

    protocolFactory := thrift.NewTCompactProtocolFactory()

    tc.client = test.NewPingServiceClientFactory(tc.transport, protocolFactory)
    
    return nil
}

func (tc *ThriftClient) retry_transport() bool {
    for {
        err := tc.transport.Open()
        if err == nil {
            fmt.Println("Connected to port", tc.port)
            return true
        }

        if tErr, ok := err.(thrift.TTransportException); ok && tErr.TypeId() == thrift.NOT_OPEN {
            fmt.Printf("ERROR: Connection refused. Unable to open transport on %d.\n", tc.port)
            time.Sleep(1 * time.Second) 
        } else {
            fmt.Printf("retry_transport status %d : %s\n", tErr.TypeId(), tErr.Error())
            return false
        }
    }
}

func (tc *ThriftClient) close_connection() {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	if tc.transport != nil {
		tc.transport.Close()
	}
}

func (tc *ThriftClient) send_ping(msg string) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

    for {
        if tc.transport == nil || !tc.transport.IsOpen() {
            fmt.Println("Transport is not open, try reconnecting")
            tc.retry_transport()
        } else {
            break
        }
    }

	start := time.Now()
	pingResponse, err := tc.client.Ping(context.Background(), msg)
    duration := time.Since(start).Nanoseconds()
	if err != nil {
		if tErr, ok := err.(thrift.TTransportException); ok {
            fmt.Printf("PING ERROR: %v\n", tErr)
            fmt.Println("BROKEN PIPE DETECTED, TRYING TO RECONNECT NOW")
            tc.reconnect()
        } else {
            fmt.Printf("Unexpected error during Ping: %v\n", err)
        }
		return
	}
	fmt.Printf("Response from server on port %d: %s\n", tc.port, pingResponse)
	fmt.Printf("Ping %d Call Duration: %d ns\n", tc.port, duration)
}


func (tc *ThriftClient) reconnect() {
	tc.close_connection()
    tc.socket = nil
	tc.transport = nil
	tc.protocol = nil
	tc.client = nil            
	tc.init_client() 
	tc.retry_transport()
}



func main() {
    str128k := generate_random_string(128)
    str256k := generate_random_string(256)
    str512k := generate_random_string(512)
    str1024k := generate_random_string(1024)
    rand.Seed(time.Now().UnixNano()) 

    var wg sync.WaitGroup
    wg.Add(1)

    startServer("9092", &wg)
    time.Sleep(1 * time.Second) 

    var handler9090, handler9091 *ThriftClient

    go func() {
        var err error
        for {
            handler9090, err = NewThriftClient(9090)
            if err == nil {
                fmt.Println("Client handler9090 connected successfully")
                break
            } else {
                fmt.Println("Failed to connect handler9090, retrying...")
                time.Sleep(1 * time.Second) 
            }
        }
    }()

    go func() {
        var err error
        for {
            handler9091, err = NewThriftClient(9091)
            if err == nil {
                fmt.Println("Client handler9091 connected successfully")
                break
            } else {
                fmt.Println("Failed to connect handler9091, retrying...")
                time.Sleep(1 * time.Second) 
            }
        }
    }()

    wg.Wait()
    for {
        for _, msg := range []string{str128k, str256k, str512k, str1024k} {
            handler9090.send_ping(msg);
            handler9091.send_ping(msg);
        }
        time.Sleep(1 * time.Second) // 避免刷屏
    }
}
