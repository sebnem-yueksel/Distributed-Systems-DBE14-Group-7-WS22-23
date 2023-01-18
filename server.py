import json
import socket
import sys
import time
from time import sleep
import threading

# Listening ports for all sockets
DYNAMIC_DISCOVERY_PORT = 57697
MESSAGE_TO_SERVER_PORT = 57777
HB_PORT = 59001
TCP_LISTENER_PORT = 10005
BROADCAST_PORT_CHAT = 58002
ELECTION_PORT = 10001

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
servers = [s_address]

# Global variables for leadership, heartbeat and voting
leader = None
isLeader = False
neighbor = None
participant = False


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
    for i in range(0, 3):
        b_socket.sendto(str.encode(BROADCAST_MESSAGE), ('<broadcast>', DYNAMIC_DISCOVERY_PORT))
        print("Searching for Leaderserver")
        # Waiting one second for the receiving (settimeout() for the socket - time.sleep for the program)
        b_socket.settimeout(1)
        time.sleep(1)

        try:
            data, address = b_socket.recvfrom(1024)
            # Continue if the leader sends a specific message back
            if data and data.startswith(str.encode(BROADCAST_ANSWER_SERVER)):
                print("successful Connected")
                response = True
                # Adds the IP of the leader to the list
                servers.append(address[0])
                # A neighbor is selected
                get_environment()
                break

        except TimeoutError:
            pass
    b_socket.close()
    # If no incoming answers, no leader exists so the server is the leader
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
    # The leader is the discovery listener
    if s_address == leader:
        b_listener.bind((s_address, DYNAMIC_DISCOVERY_PORT))
        print(f'Servers address as broadcast listener: {s_address}')

    while True:
        # Closing the listener if the server is not the leader
        if s_address != leader:
            print('Broadcast listener closing')
            b_listener.close()
            break

        try:
            # receiving messages through defined port
            message, address = b_listener.recvfrom(BUFFER_SIZE)
        except TimeoutError:
            pass
        else:
            if message and leader == s_address:
                # Leader sends an answer will be sent as soon as a message has been received through the port
                b_listener.sendto(str.encode(BROADCAST_ANSWER_SERVER), address)

                # Distinction whether a client or a server wants to be added
                # When a defined message is received from a server, it is added to the server list
                if message and message.startswith(str.encode(BROADCAST_MESSAGE)):
                    print(f'server {address[0]} would like to join the chat')
                    if address[0] not in servers:
                        servers.append(address[0])
                        # The get_environment function is activated to find the neighbor
                        get_environment()
                        # Other servers will be contacted by sending the IP address of the new server
                        message_to_server(servers)
                        # If an election has to be performed after receiving a new server
                        # The next two lines can be used for this purpose
                        # time.sleep(5)
                        # leader_election()
                # When a defined message is received from a server, it will print the line 124
                if message and message.startswith(str.encode(BROADCAST_MESSAGE_CLIENT)):
                    print(f'client {address[0]} would like to join the chat')


# As soon as a new server join the application the leader sends via broadcast an information to the other servers
# The updated list is sent
def message_to_server(message):
    # list is joined to one string to be able to be sent
    server_list = ' '.join(message)
    sender_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sender_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    sender_socket.sendto(server_list.encode(), ('<broadcast>', MESSAGE_TO_SERVER_PORT))
    print('Other servers informed')
    sender_socket.close()


# listener for incoming messages from servers to get the updated server list
def message_from_server():
    listen_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    listen_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    listen_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listen_socket.bind((s_address, MESSAGE_TO_SERVER_PORT))

    while True:
        try:
            data, address = listen_socket.recvfrom(BUFFER_SIZE)
            if data:
                # received one string list is split to a normal list
                server_list = (data.decode().split(' '))
                # Update the list
                servers[:] = server_list
                print(f'new list available {servers}')
                time.sleep(0.2)
        except ConnectionRefusedError:
            pass


# Method to check if the neighbor is still available
def heartbeat_sender():
    message = '#'
    ping = str.encode(message)
    heartbeat = 0
    while True:
        if neighbor:
            try:
                # Message is sent via TCP socket to the neighbor in a second cycle
                # If no connection could be established, the heartbeat is counted up
                heartbeat_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                heartbeat_socket.settimeout(1)
                heartbeat_socket.connect((neighbor, HB_PORT))
                heartbeat_socket.send(ping)
                heartbeat_socket.close()
                sleep(1)
            # No incrementing of the heartbeat when connecting
            except (ConnectionRefusedError, TimeoutError):
                heartbeat += 1
            else:
                heartbeat = 0
            # With more than 5 heartbeats the neighbor counts as crashed

            # Checks if this neighbor was the leader, if so a new election is started
            if heartbeat > 5:
                heartbeat = 0
                print(f'failed heartbeats to neighbor, remove {neighbor} and get a new environment')
                # This neighbor is removed from the list, all servers are informed and a new neigbor is selected
                servers.remove(neighbor)
                message_to_server(servers)
                # check if the neighbor was the leader
                neighbor_leader = neighbor == leader
                time.sleep(2)
                get_environment()
                # if a leader crashes, the election is started
                if neighbor_leader:
                    print('Previous neighbor was leader, starting election')
                    time.sleep(2)
                    leader_election()
                    discovery_listener()

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
    sorted_binary_ring = sorted([socket.inet_aton(member) for member in server_list])
    sorted_ip_ring = [socket.inet_ntoa(node) for node in sorted_binary_ring]

    return sorted_ip_ring

# Method to find a neighbor from the server list
def get_environment():
    global neighbor
    # If there is only one server in the list, there is no neighbor
    if len(servers) == 1:
        neighbor = None
        print('No neighbor available')
        return
    # The list servers is sorted by the method form_ring()
    form_ring(servers)
    time.sleep(1)
    # searches position of the server in the server list
    index = servers.index(s_address)
    # Determines the next element in the list servers after the element whose position is stored in the variable index.
    neighbor = servers[0] if index + 1 == len(servers) else servers[index + 1]
    print(f'New neighbor: {neighbor}')


# Method to start the leader election
def leader_election(address=s_address):
    # Global variable for using them in the hole program
    global leader, isLeader, participant
    election_message = {'mid': s_address, 'isLeader': False}

    # if only one server in the server list
    if len(servers) == 1:
        print(f'I am the only server in the system')
        leader = s_address

    # Start the election if more than one server in the server list
    elif len(servers) > 1:
        print('start election')
        participant = True
        # the election message is sent via broadcast to the neighbor
        # Using JSON to send the message because it is a dictionary
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
        # Receiving messages by using JSON
        election, neighbor_right = election_socket.recvfrom(BUFFER_SIZE)
        election_message = json.loads(election.decode('utf-8'))

        # If an election message is received, compare the received IP with s_address and the leader status
        if election:
            # First set the leader false
            leader = False

            # If the isLeader status is True, the received IP is the new leader
            if election_message['isLeader']:
                leader = election_message['mid']
                time.sleep(2)
                participant = False
                # The new received message is forwarded to the neighbor
                leader_message = json.dumps(election_message)
                time.sleep(1)
                election_socket.sendto(leader_message.encode(), (neighbor, ELECTION_PORT))
                # Update the discovery_listener
                discovery_listener()

            # If the received IP is smaller than s_address and not participant
            elif election_message['mid'] < s_address and not participant:
                # The election messages is updated with the current IP (s_address) and is sent to the neighbor
                new_election = {'mid': s_address, 'isLeader': False}
                new_election = json.dumps(new_election)
                participant = True
                time.sleep(1)
                election_socket.sendto(new_election.encode(), (neighbor, ELECTION_PORT))

            # If the received IP is greater than s_address, the election messages is forwarded to the neighbor
            # Election message is not updated
            elif election_message['mid'] > s_address:
                participant = True
                message = json.dumps(election_message)
                time.sleep(1)
                election_socket.sendto(message.encode(), (neighbor, ELECTION_PORT))

            # if the received IP is equal to s_address the election messages is updated with isLeader=True
            # Updated message is sent to the neighbor
            elif election_message['mid'] == s_address:
                # Update the dynamic_listener
                discovery_listener()
                time.sleep(1)
                # Set the new leader
                leader = s_address
                new_election = {'mid': s_address, 'isLeader': True}
                new_election = json.dumps(new_election)
                # set participant False
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
