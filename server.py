import json
import socket
import sys
import time
from time import sleep
import threading

# Listening port Broadcasts for all sockets
DYNAMIC_DISCOVERY_PORT = 57697
MESSAGE_TO_SERVER_PORT = 57777
HB_PORT = 59001
TCP_LISTENER_PORT = 10005
BROADCAST_PORT_CHAT = 58002
ELECTION_PORT = 10001

participant = False

# Buffer size
BUFFER_SIZE = 5120

# Messages for identification
BROADCAST_MESSAGE = 'Hi'
BROADCAST_ANSWER_SERVER = 'Welcome'
BROADCAST_MESSAGE_CLIENT = 'Could I join the Chatroom?'

# Local host information
MY_HOST = socket.gethostname()
s_address = socket.gethostbyname(MY_HOST)

# List of componentes
clients = []
servers = [s_address]

# Variables for leadership and voting
leader = None
isLeader = False
neighbor = None


# Beginning of the system and dynamic discovery for incoming servers
# both are done via UDP broadcast sockets
def discovery_broadcast():
    # create UDP socket
    b_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    b_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    b_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    b_socket.bind((s_address, DYNAMIC_DISCOVERY_PORT))
    response = False

    # the server tries to connect to an existing leader server in the application
    # in this application the leader servers will receive the broadcast messages
    # Three attempts to reach the leader by sending a  defined message
    # Waiting one second for the receiving
    # Continue if the leader sends a specific message back
    # Adds the IP of the leader to the list
    # A neighbor is selected
    for i in range(0, 3):
        b_socket.sendto(str.encode(BROADCAST_MESSAGE), ('<broadcast>', DYNAMIC_DISCOVERY_PORT))
        print("Searching for Leaderserver")
        b_socket.settimeout(1)
        time.sleep(1)

        try:
            data, address = b_socket.recvfrom(1024)
            if data and data.startswith(str.encode(BROADCAST_ANSWER_SERVER)):
                print("successful Connected")
                response = True
                servers.append(address[0])
                get_environment()
                break

        except TimeoutError:
            pass

    b_socket.close()
    # If no incoming answers, no leader exists so the server is the leader -> ETWAS UMGESTALTEN
    if not response:
        print('no success')
        leader_election()


# Method for listening to incoming messages for new servers and clients
# This method will be only used by the leader who waits for specific incoming messages
def discovery_listener():
    # create UDP socket
    b_listener = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    b_listener.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    b_listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    if s_address == leader:
        b_listener.bind((s_address, DYNAMIC_DISCOVERY_PORT))
        print(f'Servers address as broadcast listener: {s_address}')

    while True:
        if s_address != leader:
            print('Broadcast listener closing')
            b_listener.close()
            break

        try:
            # receiving messages through defined port
            message, address = b_listener.recvfrom(BUFFER_SIZE)
        except TimeoutError:  # REICHT DAS ALS FEHLERMELDUNG?
            pass
        else:
            if message and leader == s_address:
                # Leader sends an answer will be sent as soon as a message has been received through the port
                b_listener.sendto(str.encode(BROADCAST_ANSWER_SERVER), address)

                # Distinction whether a client or a server wants to be added
                # When a defined message is received from a server, it is added to the server list
                # the get_environment function is activated
                # other servers will be contacted by sending the IP address of the new server
                if message and message.startswith(str.encode(BROADCAST_MESSAGE)):
                    print(f'server {address[0]} would like to join the chat')
                    if address[0] not in servers:
                        servers.append(address[0])
                        get_environment()
                        message_to_server(servers)
                        time.sleep(5)
                       # leader_election()
                if message and message.startswith(str.encode(BROADCAST_MESSAGE_CLIENT)):
                    print(f'client {address[0]} would like to join the chat')


# As soon as a new server join the application the leader sends via broadcast ann information to the other servers
# The IP address of the new server will be sent
def message_to_server(message):
    server_list = ' '.join(message)
    sender_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sender_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    sender_socket.sendto(server_list.encode(), ('<broadcast>', MESSAGE_TO_SERVER_PORT))
    print('Other servers informed')
    sender_socket.close()


# Servers that are not a leader waits for incoming messages from the leader
# As soon as they receive the address from a new server, this address will be added to the server list
def message_from_server():
    listen_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    listen_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    listen_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listen_socket.bind((s_address, MESSAGE_TO_SERVER_PORT))

    while True:
        try:
            data, address = listen_socket.recvfrom(BUFFER_SIZE)

            if data:
                server_list = (data.decode().split(' '))
                servers[:] = server_list
                print(f'new list available {servers}')
                time.sleep(0.2)
        except ConnectionRefusedError:
            pass


# Method to check if servers are available
def heartbeat_sender():
    message = '#'
    ping = str.encode(message)
    heartbeat = 0
    while True:
        if neighbor:
            try:
                # Message is sent via TCP socket to the neighbor in a second cycle
                # If no connection could be established, the heartbeat is counted up
                # No incrementing of the heartbeat when connecting
                heartbeat_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                heartbeat_socket.settimeout(2)
                heartbeat_socket.connect((neighbor, HB_PORT))
                heartbeat_socket.send(ping)
                heartbeat_socket.close()
                sleep(1)
            except (ConnectionRefusedError, TimeoutError):
                heartbeat += 1
            else:
                heartbeat = 0
            # MUSS NOCH ETWAS ANGEPASST WERDEN
            # With more than 4 heartbeats the neighbor counts as crashed
            # This neighbor will be removed and a new neighbor is selected
            # Checks if this neighbor was the leader, if so a new election is started
            if heartbeat > 5:
                heartbeat = 0
                print(f'failed heartbeats to neighbor, remove {neighbor} and get a new environment')
                servers.remove(neighbor)
                message_to_server(servers)
                neighbor_leader = neighbor == leader
                time.sleep(2)
                get_environment()
                if neighbor_leader:
                    print('Previous neighbor was leader, starting election')
                    time.sleep(2)
                    leader_election()
                    # discovery_listener()

    print('Heartbeat thread closing')
    sys.exit(0)


# Listener to heartbeat_sender
def heartbeat_listener():
    # Create TCP socket to listen so heartbeats from neighbor
    heartbeat_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    heartbeat_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    heartbeat_socket.bind((s_address, HB_PORT))
    heartbeat_socket.settimeout(4)

    while True:
        try:
            heartbeat_socket.listen()
            heartbeat, heartbeat_neighbour = heartbeat_socket.accept()
            # Continue by receiving heartbeats
            if heartbeat:
                time.sleep(1)
        except TimeoutError:
            pass


# Form rings for finding neighbor
# Basic from practical codes
def form_ring(server_list):
    global election
    sorted_binary_ring = sorted([socket.inet_aton(member) for member in server_list])
    sorted_ip_ring = [socket.inet_ntoa(node) for node in sorted_binary_ring]

    return sorted_ip_ring


# Method to find a neighbor from the server list
# If there is only one server in the list, there is no neighbor
# Determines the next element in the list servers after the element whose position is stored in the variable index.
# If the value of index plus 1 is equal to the length of the list servers
# the first element of the list is assigned to the variable neighbor (servers[0])
# otherwise the element at position index + 1 is assigned (servers[index + 1]).

def get_environment():
    global neighbor
    quantity = len(servers)
    if quantity == 1:
        neighbor = None
        print('No neighbor available')
        return
    form_ring(servers)
    index = servers.index(s_address)  # searchs position of the server in the server list
    neighbor = servers[0] if index + 1 == quantity else servers[index + 1]
    print(f'New neighbor: {neighbor}')


def leader_election(address=s_address):
    global leader, isLeader, participant
    leader = address
    participant = False
    election_message = {'mid': s_address, 'isLeader': False}

    if len(servers) == 1:
        print(f'I am the only server in the system')
        leader = s_address

    elif len(servers) > 1:
        print('start election')
        participant = True

        election_message = json.dumps(election_message)
        election_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            election_socket.sendto(election_message.encode(), (neighbor, ELECTION_PORT))

        except ConnectionRefusedError:
            pass


def election_listener():
    global leader, isLeader, participant
    election_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    election_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    election_socket.bind(('', ELECTION_PORT))

    while True:
        time.sleep(1)
        election, neighbor_right = election_socket.recvfrom(BUFFER_SIZE)
        election_message = json.loads(election.decode('utf-8'))

        if election:
            leader = False

            if election_message['isLeader']:
                leader = election_message['mid']
                # set_server_leader(leader)
                #print(f'the leader is {leader}')
                time.sleep(2)
                participant = False
                leader_message = json.dumps(election_message)
                time.sleep(1)
                election_socket.sendto(leader_message.encode(), (neighbor, ELECTION_PORT))
                discovery_listener()
                #sys.exit(0)

            elif election_message['mid'] < s_address and not participant:
                new_election = {'mid': s_address, 'isLeader': True}
                new_election = json.dumps(new_election)
                participant = True
                time.sleep(1)
                election_socket.sendto(new_election.encode(), (neighbor, ELECTION_PORT))

            elif election_message['mid'] > s_address:
                participant = True
                message = json.dumps(election_message)
                time.sleep(1)
                election_socket.sendto(message.encode(), (neighbor, ELECTION_PORT))

            elif election_message['mid'] == s_address:
                discovery_listener()
                time.sleep(1)
                leader = s_address
                new_election = {'mid': s_address, 'isLeader': True}
                new_election = json.dumps(new_election)
                participant = False
                election_socket.sendto(new_election.encode(), (neighbor, ELECTION_PORT))


# Methods for the chat
# Implementation a TCP listener for incoming messages from clients
def tcp_listener():
    # Start listening
    # create socket
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    # bind the socket to specific address and port
    s.bind((s_address, TCP_LISTENER_PORT))
    # listen for incoming connections
    s.listen()
    print('Start listening to clients')
    while True:

        try:
            # wait for client to connect
            data, client_address = s.accept()
            if data:
                # receive data
                text = data.recv(BUFFER_SIZE).decode()
                # print the source address of the incoming message + the received data
                print(f'incoming messages from ' + text)
                # send the reveived data to function
                broadcastsender_chat(text)

        except TimeoutError:
            pass
        # close socket
        except ConnectionRefusedError:
            s.close()


def broadcastsender_chat(message):
    bmessage = message.encode()

    b_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    # Set the socket to broadcast and enable reusing addresses
    b_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    b_socket.sendto(bmessage, ('255.255.255.255', BROADCAST_PORT_CHAT))
    b_socket.close()


if __name__ == '__main__':
    # send out a broadcast message to discover other Servers or clients on the network
    discovery_broadcast()
    # create a separate thread to run the broadcast listener function
    threadBL = threading.Thread(target=discovery_listener)
    threadBL.start()
    # create a separate thread to run message from server function
    threadMS = threading.Thread(target=message_from_server)
    threadMS.start()
    threadEL = threading.Thread(target=election_listener)
    threadEL.start()
    # create a separate thread to run the heartbeat function
    threadHB = threading.Thread(target=heartbeat_sender)
    threadHB.start()
    # create a separate thread to run the heartbeat Listen function
    threadHL = threading.Thread(target=heartbeat_listener)
    threadHL.start()
    # create a separate thread to run the TCP listener function
    threadTL = threading.Thread(target=tcp_listener)
    threadTL.start()
