import json 
import socket
import threading


lock=threading.RLock()
connection={}


class UpstreamConnectionThread(threading.Thread):
    
    def __init__(self,client_sock,server_sock):
        threading.Thread.__init__(self)
        self.client_sock=client_sock
        self.server_sock=server_sock
        self.client_buffered_data=b''
        self.kill = threading.Event()
        self.suicide=False
        self.downstream_connection_thread=None

    def closeClientSocket(self):
        try:
            self.client_sock.close()
            print('closing client socket')
        except:
            print('client socket already closed')

    def closeServerSocket(self):
        try:
            self.server_sock.close()
            print('closing server socket')
        except:
            print('server socket already closed')  

    
    def receiveFromClient(self, max_len=1000):
        """Receive packets from client connection"""

        try:
            data = self.client_sock.recv(max_len)
            print("reciving from client")
            print(data)
            return data
        except socket.timeout:
            self.closeServerSocket()
            self.downstream_connection_thread.kill.set()
            self.suicide=True
            

    def sendToServer(self, data, flush = True):
        """Send packets to server connection"""
        try:
            self.server_sock.send(data)
            print('sending to server')
            print(data)
        except:
            print('server disconnected')
            self.closeClientSocket()
            self.downstream_connection_thread.kill.set()
            self.suicide=True


    def run(self):
        """runs at the start of the thread"""
        with open('log.txt','a') as fo:
            fo.write("initaiating "+str(threading.currentThread().getName())+"\n")
        
        while not self.kill.is_set() and self.suicide == False:

            #step 1
            #incoming client request
            incoming_client_data= self.receiveFromClient()
            print("reciving from client")
            print(incoming_client_data)
            self.client_buffered_data+=incoming_client_data
            self.sendToServer(incoming_client_data)


        with open('log.txt','a') as fo:
            fo.write("destroying upstream conn"+str(threading.currentThread().getName())+"\n")
        return


class DownstreamConnectionThread(threading.Thread):

    def __init__(self,client_sock,server_sock):
        threading.Thread.__init__(self)
        self.client_sock=client_sock
        self.server_sock=server_sock
        self.server_buffered_data=b''
        self.kill=threading.Event()
        self.suicide=False
        self.upstream_connection_thread=None

    def closeClientSocket(self):
        try:
            self.client_sock.close()
            print('closing client socket')
        except:
            print('client socket already closed')
    
    def closeServerSocket(self):
        try:
            self.server_sock.close()
            print('closing server socket')
        except:
            print('server socket already closed')

    # def shouldCloseConnection(self):
    #     """check whether we should close the sever connection based on header connection value"""
    #     headers_encoded=self.server_buffered_data.split(b'\r\n\r\n')
    #     self.server_buffered_data=b''
    #     if len(headers_encoded)>0:
    #         headers_encoded_list=headers_encoded[0].split(b'\r\n')
    #         for header_encoded in headers_encoded_list:
    #             header_mapping_encoded=header_encoded.split(b': ')
    #             if len(header_mapping_encoded)>1:
    #                 if header_mapping_encoded[0].decode().lower()=='connection':
    #                     if header_mapping_encoded[1].decode().lower()=='close':
    #                         return 1
    #                     elif header_mapping_encoded[1].decode().lower()=='keep-alive':
    #                         return 0
    #     return 0


    def receiveFromServer(self, max_len=1000):
        """Receive packets from server connection"""

        try:
            data = self.server_sock.recv(max_len)
            print("reciving from server")
            print(data)
            return data
        except socket.timeout:
            self.closeClientSocket()
            self.upstream_connection_thread.kill.set()
            self.suicide=True

    def sendToClient(self, data, flush = True):
        """Send packets to client connection"""
        try:
            self.client_sock.send(data)
            print('sending to client')
            print(data)
        except:
            print('client disconnected')
            self.closeServerSocket()
            self.upstream_connection_thread.kill.set()
            self.suicide=True

    
    def run(self):
        """runs at the start of the thread"""
        with open('log.txt','a') as fo:
            fo.write("initaiating "+str(threading.currentThread().getName())+"\n")

        while not self.kill.is_set() and self.suicide==False:

            #step3
            #incoming server response
            incoming_server_data=self.receiveFromServer()
            self.server_buffered_data+=incoming_server_data  

            #step4
            #outgoing client response
            self.sendToClient(incoming_server_data)

            #check whether the connection should be alive
            if incoming_server_data==b'':
                self.upstream_connection_thread.kill.set()
                self.closeServerSocket()
                self.closeClientSocket()

                with open('log.txt','a') as fo:
                    fo.write("destroying downstream conn "+str(threading.currentThread().getName())+"\n")

                return

        with open('log.txt','a') as fo:
            fo.write("destroying downstream conn "+str(threading.currentThread().getName())+"\n")
        return

        



                
def openServerConnection(server_ip,server_port):
        """connects to a server"""

        sock2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock2.settimeout(10)

        try:
            sock2.connect((server_ip,server_port))
            sock2.setblocking(1)

        except socket.error:
            print('cannot connect to specified socket.')
            return sock2,False

        return sock2,True


    


def getServerConnection(server_ip,server_port):

    "the sever choosing logic"
    return openServerConnection(server_ip,server_port)

        

def listenOnPort(port,server_ip,server_port):
    global connection

    sock=socket.socket()
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(('127.0.0.1',port))
    sock.listen(port)
    sock.settimeout(10)
    (ip,port)=sock.getsockname()

    print("listening at port "+str(port))

    while True:
        sock.setblocking(1)
        client_sock, clientAddress = sock.accept()
        server_sock,is_connected=getServerConnection(server_ip,server_port)
        print(server_ip,server_port)
        if is_connected==False:
            try:
                client_sock.close()
                print('closing client socket')

            except:
                print('already closed')
        else:    
            with open('log.txt','a') as fo:
                fo.write("established new connection\n")
                fo.write("client_sock"+str(client_sock)+"\n")
                fo.write("server_sock"+str(server_sock)+"\n")

            upstream_connection = UpstreamConnectionThread(client_sock,server_sock)
            downstream_connection = DownstreamConnectionThread(client_sock,server_sock)
            upstream_connection.downstream_connection_thread=downstream_connection
            downstream_connection.upstream_connection_thread=upstream_connection
            
            error1=upstream_connection.start()
            error2=downstream_connection.start()
            if error1 or error2:
                raise SystemExit
            

def getConfiguration():
    """load configuration from conf.json file"""

    with open('config.json') as conf_file:
        conf=json.loads(conf_file.read())
        return conf



if __name__=='__main__':
    
    print("starting BalanceTheLoad....")

    conf=getConfiguration()
    listenOnPort(conf['listen_on'],conf['workers'][0]['ip_address'],conf['workers'][0]['port'])

