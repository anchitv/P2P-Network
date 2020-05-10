import sys
import threading
import time
from socket import *



class Peer():
    def __init__(self):
        super().__init__()
        self.client_socket_udp = socket(AF_INET, SOCK_DGRAM)
        self.server_socket_udp = socket(AF_INET, SOCK_DGRAM)
        self.server_socket_tcp = socket(AF_INET, SOCK_STREAM)
        self.udp_server_thread = threading.Thread(target=self.server_handler_udp)
        self.tcp_server_thread = threading.Thread(target=self.server_handler_tcp)
        self.client_thread = threading.Thread(target=self.client_handler)
        self.input_thread = threading.Thread(target=self.read_input)
        self.kill_thread = False

    def initialise(self, peer, first_successor, second_successor, ping_interval, threads):
        # Initialise the peer by setting up its successors and starting threads for servers and client

        self.peer = int(peer)
        self.first_successor = int(first_successor)
        self.second_successor = int(second_successor)
        print("My new first successor is Peer {}".format(self.first_successor))
        print("My new second successor is Peer {}".format(self.second_successor))

        self.ping_interval = int(ping_interval)
        self.start_threads(threads)
        self.files = []
        return

    def start_threads(self, threads):
        # Start UDP server and client and TCP server on threads

        if threads=="server" or threads=="all":
            self.tcp_server_thread.start()
            self.udp_server_thread.start()
            self.input_thread.start()
        
        if threads=="client" or threads=="all":
            time.sleep(1)
            self.client_thread.start()
        return

    def stop_threads(self):
        # Stop the threads that are running

        self.kill_thread = True
        self.server_socket_udp.close()
        self.server_socket_tcp.close()
        print("UDP Server closed")
        print("TCP Server closed")
        return

    def server_handler_udp(self):
        # UDP Server thread function.

        self.server_socket_udp.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        self.server_socket_udp.bind(("localhost", self.peer+12000))
        print('UDP Server is ready for service')

        while True and not self.kill_thread:
            message, clientAddress = self.server_socket_udp.recvfrom(2048)
            message = message.decode()
            # print(message)
            print('Ping request message received from Peer', message)
            if message:
                server_message = str(self.peer)
            self.server_socket_udp.sendto(server_message.encode(), clientAddress)

        self.server_socket_udp.close()
        print("UDP Server closed")
        return

    def client_handler(self):
        # UDP Client thread function.

        message = str(self.peer)
        heartbeat = {
                    self.first_successor: 0,
                    self.second_successor: 0
                    }

        while True and not self.kill_thread:
            print('Ping requests sent to Peers {} and {}'.format(self.first_successor, self.second_successor))

            successors = [self.first_successor, self.second_successor]
            heartbeat = {k:v for k,v in heartbeat.items() if k in successors}

            for peer in successors:
                if peer not in heartbeat:
                    heartbeat[peer] = 0

                try:
                    self.client_socket_udp.settimeout(2)
                    # Heartbeat of Successors
                    self.client_socket_udp.sendto(message.encode(),("localhost", int(peer)+12000))
                    #wait for the reply from the server
                    receivedMessage, addr = self.client_socket_udp.recvfrom(2048)
                    receivedMessage = receivedMessage.decode()

                    print("Ping response received from Peer {}".format(peer))
                    heartbeat[peer] = 0

                except timeout:
                    print("Heartbeat missing with Peer {}".format(peer))
                    heartbeat[peer] += 1
                    print(heartbeat)
                    if heartbeat[peer]>2:
                        self.remove_abrupt(peer)

            time.sleep(self.ping_interval)

        self.client_socket_udp.close()
        print("UDP Client closed")
        return

    def server_handler_tcp(self):
        # TCP Server function.

        self.server_socket_tcp.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        self.server_socket_tcp.bind(('localhost', self.peer+12000))
        self.server_socket_tcp.listen(5)
        print('TCP Server is ready for service at {}'.format(self.peer+12000))

        while True and not self.kill_thread:
            conn, addr = self.server_socket_tcp.accept()
            message, clientAddress = conn.recvfrom(2048)
            #received data from the client, now we know who we are talking with
            message = message.decode()
            
            # print('TCP ping request received from Peer {}'.format(conn))
            reply = self.process_tcp_request(message)
            if reply:
                conn.send(reply.encode())
            conn.close()
        
        self.server_socket_tcp.close()
        print("TCP Server closed")
        return
    
    def process_tcp_request(self, message):
        # Process the TCP request

        decoded_message = message.split()
        message_action = decoded_message[0]
        # message_info = int(decoded_message[1])
        reply = ""

        if message_action=="join":
            print("Peer {} Join request received".format(int(decoded_message[1])))
            self.join_peer(int(decoded_message[1]))

        elif message_action=="change":
            self.change_successor(int(decoded_message[1]), int(decoded_message[2]), message)
        
        elif message_action=="remove":
            self.remove_successor(int(decoded_message[1]), int(decoded_message[2]), int(decoded_message[3]), message)

        elif message_action=="Accepted":
            self.initialise(self.peer, int(decoded_message[1]), int(decoded_message[2]), self.ping_interval, "client")

        elif message_action=="store":
            self.store_file(int(decoded_message[1]), decoded_message[2])

        elif message_action=="get_successor":
            reply = self.get_successor(int(decoded_message[1]))

        elif message_action=="request":
            reply = self.request_file(int(decoded_message[1]), decoded_message[2])

        elif message_action=="get":
            reply = self.get_file(decoded_message[1])
        
        else:
            print("File received")
            self.save_received_file(message)
            
        return reply

    def tcp_request(self, dest, message):
        # Function to send a message to destination (dest) over TCP. Returns the message received.

        tcp_socket = socket(AF_INET, SOCK_STREAM)
        tcp_socket.connect(("localhost", dest+12000))
        tcp_socket.sendall(message.encode())
        # wait for the reply from the server
        receivedMessage = tcp_socket.recv(2048)
        receivedMessage = receivedMessage.decode()
        tcp_socket.close()

        return receivedMessage

    def join_request(self, peer, known_peer, ping_interval):
        # Initial request by a new peer to join DHT.

        self.peer = int(peer)
        self.ping_interval = int(ping_interval)
        tcp_socket = socket(AF_INET, SOCK_STREAM)
        self.start_threads("server")

        message = "join {}".format(peer)
        receivedMessage = self.tcp_request(known_peer, message)
        # receivedMessage = receivedMessage.split()

        # if receivedMessage[0]=="Accepted":
        #     self.initialise(peer, receivedMessage[1], receivedMessage[2], ping_interval)
        #     print("My new first successor is Peer {}".format(self.first_successor))
        #     print("My new second successor is Peer {}".format(self.second_successor))
        # else:
        #     print("Connection refused")
        
        return

    def join_peer(self, new_peer):
        # Checks where the peer has to be added or the request to add peer should be sent.

        # last node, add to current peer itself
        if self.peer > self.first_successor:
            self.add_peer(new_peer)

        # first successor is the last, add to first successor
        elif self.second_successor < self.first_successor and new_peer > self.first_successor:
            print("Peer {} Join request forwarded to my successor {}".format(new_peer, self.first_successor))
            message = "join {}".format(new_peer)
            self.tcp_request(self.first_successor, message)

        # first successor is the last, add to current peer
        elif self.second_successor < self.first_successor and new_peer > self.peer:
            self.add_peer(new_peer)

        # send request to second
        elif new_peer > self.second_successor:
            print("Peer {} Join request forwarded to my successor {}".format(new_peer, self.second_successor))
            message = "join {}".format(new_peer)
            self.tcp_request(self.second_successor, message)

        # send request to first, add to first successor
        elif new_peer > self.first_successor:
            print("Peer {} Join request forwarded to my successor {}".format(new_peer, self.first_successor))
            message = "join {}".format(new_peer)
            self.tcp_request(self.first_successor, message)

        # add to current peer
        else:
            self.add_peer(new_peer)

        return

    def add_peer(self, new_peer):
        # Add peer as a successor to current peer.

        # successor change of predecessor
        message = "change {} {}".format(self.peer, new_peer)
        self.tcp_request(self.second_successor, message)

        # Inform peer that it is accepted into DHT
        message = "Accepted {} {}".format(self.first_successor, self.second_successor)
        self.tcp_request(new_peer, message)
            
        print("My new first successor is Peer {}".format(new_peer))
        print("My new second successor is Peer {}".format(self.first_successor))

        self.second_successor = self.first_successor
        self.first_successor = new_peer

        self.transfer_files("join")

        return

    def change_successor(self, source_peer, new_successor, message):
        # Changes the successor of the peer. Happens when a new peer joins the DHT.

        if self.first_successor == source_peer:
            print("Successor Change request received")
            self.second_successor = new_successor
            print("My new first successor is Peer {}".format(self.first_successor))
            print("My new second successor is Peer {}".format(self.second_successor))
        elif self.second_successor == source_peer:
            self.tcp_request(self.first_successor, message)
        else:
            self.tcp_request(self.second_successor, message)
        return

    def read_input(self):
        # Read user inputs from command line in real-time

        while True:
            command = input()
            command = command.split()
            exit_list = ["Quit", "quit", "Exit", "exit", "Close", "close"]
            
            if not command:
                continue
            if command[0] in exit_list:
                print("exiting...\n")
                self.remove_graceful()
                print("Exit successful")
            elif command[0].lower()=="store":
                print("storing file...\n")
                message = "store {} {}".format(self.peer, command[1])
                print("File Store {} request forward to my first successor".format(command[1]))
                self.tcp_request(self.first_successor, message)
            elif command[0].lower()=="request":
                print("retreiving file...\n")
                self.request_file(self.peer, command[1])
            else:
                print("Yeah.")

        return

    def get_successor(self, successor):
        # Return the required sucessor

        if successor == 1:
            reply = self.first_successor
        elif successor == 2:
            reply = str(self.second_successor)
        return str(reply)        

    def remove_successor(self, gone_peer, new_first_successor, new_second_successor, message):
        # Check if successor change due to peer departure is required
        # Transfer message forward otherwise

        if self.first_successor == gone_peer:
            self.first_successor = new_first_successor
            self.second_successor = new_second_successor
            print("Peer {} will depart from the network".format(gone_peer))
            print("My new first successor is Peer {}".format(self.first_successor))
            print("My new second successor is Peer {}".format(self.second_successor))
        elif self.second_successor == gone_peer:
            self.second_successor = new_first_successor
            print("Peer {} will depart from the network".format(gone_peer))
            print("My new first successor is Peer {}".format(self.first_successor))
            print("My new second successor is Peer {}".format(self.second_successor))
            self.tcp_request(self.first_successor, message)
        else:
            self.tcp_request(self.first_successor, message)
        return

    def remove_graceful(self):
        # Peer departs gracefully

        message = "remove {} {} {}".format(self.peer, self.first_successor, self.second_successor)
        self.tcp_request(self.first_successor, message)
        self.transfer_files("leave")
        self.stop_threads()
        return

    def remove_abrupt(self, peer):
        # Remove a successor with no heartbeat

        print("Peer {} is no longer alive".format(peer))
        if peer == self.first_successor:
            self.first_successor = self.second_successor

        message = "get_successor 1"
        received_messsage = self.tcp_request(self.first_successor, message)
        self.second_successor = int(received_messsage)
        print("My new first successor is Peer {}".format(self.first_successor))
        print("My new second successor is Peer {}".format(self.second_successor))
        
        return

    def hash_file(self, filename):
        return int(filename)%256

    def get_file(self, filename):
        with open(filename, "r") as f:
            content = f.read()
        return content

    def save_file(self, peer_with_file, filename):
        # Get file from peer and save it locally

        message = "get {}".format(filename)
        content = self.tcp_request(peer_with_file, message)
        
        with open (str(filename), 'w+') as f:
            f.write(content)
        self.files.append(filename)
        print("File saved")
        return

    def store_file(self, peer_with_file, filename):
        # Store file in network based on hash value

        file_peer = self.hash_file(filename)
        message = "store {} {}".format(peer_with_file, filename)

        if file_peer == self.peer:
            print("File Store {} request accepted".format(filename))
            self.save_file(peer_with_file, filename)

        elif file_peer > self.peer:
            # current peer is last peer
            if self.first_successor < self.peer:
                print("File Store {} request accepted".format(filename))
                self.save_file(peer_with_file, filename)

            # current peer is second last peer
            elif self.second_successor < self.first_successor:
                print("File Store {} request forward to my first successor".format(filename))
                self.tcp_request(self.first_successor, message)
            
            # store in first successor
            elif file_peer <= self.first_successor:
                print("File Store {} request forward to my first successor".format(filename))
                self.tcp_request(self.first_successor, message)

        if file_peer < self.peer and self.first_successor > self.peer:
            print("File Store {} request accepted".format(filename))
            self.save_file(peer_with_file, filename)
        
        elif file_peer < self.first_successor:
            print("File Store {} request forward to my first successor".format(filename))
            self.tcp_request(self.first_successor, message)
            
        else:
            print("File Store {} request forward to my second successor".format(filename))
            self.tcp_request(self.second_successor, message)

        return

    def request_file(self, peer_requiring_file, filename):
        # Find peer with file
        
        message = "request {} {}".format(peer_requiring_file, filename)
        file_peer = self.hash_file(filename)
        
        if filename in self.files:
            print("File {} is stored here".format(filename))
            self.send_file(peer_requiring_file, filename)
        else:
            print("Request for File {} has been received, but the file is not stored here".format(filename))
            self.tcp_request(self.first_successor, message)

        return

    def save_received_file(self, content):
        # Save received file with custom name

        filename = time.strftime("received_%y%m%d_%H%M%S")
        with open (filename, 'w+') as f:
            f.write(content)
        self.files.append(filename)
        print("File saved")
        return
    
    def send_file(self, peer_requiring_file, filename):
        # Returns content of file

        print("Sending file {} to Peer {}".format(filename, peer_requiring_file))
        with open(filename, "r") as f:
            content = f.read()
        print("The file has been sent")
        self.tcp_request(peer_requiring_file, content)
        return

    def transfer_files(self, condition):
        # Transfer files when leaving network

        # transfer appropriate files to newly joined peer
        if condition=="join":
            for filename in self.files:
                file_hash = self.hash_file(filename)
                if file_hash < self.first_successor and file_hash > self.peer:
                    self.store_file(self.peer, filename)
        
        # transfer all files to appropriate peer
        elif condition=="leave":
            for filename in self.files:
                message = "store {} {}".format(self.peer, filename)
                self.tcp_request(self.first_successor, message)
        else:
            print("What do you mean {}?".format(condition))
               
        return




def main():
    call_type = sys.argv[1]
    peer_name = int(sys.argv[2])

    if call_type=="init":
        first_successor = sys.argv[3]
        second_successor = sys.argv[4]
        ping_interval = sys.argv[5]
        print(call_type, peer_name, first_successor, second_successor, ping_interval)
        
        peer = Peer()
        peer.initialise(peer_name, first_successor, second_successor, ping_interval, "all")

    elif call_type=="join":
        known_peer = int(sys.argv[3])
        ping_interval = int(sys.argv[4])
        peer = Peer()
        peer.peer = peer_name
        peer.join_request(peer_name, known_peer, ping_interval)

    return


if __name__ == "__main__":
    main()
