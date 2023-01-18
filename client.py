import socket
import sys
import threading

BROADCAST_PORT = 57697
TCP_PORT = 10005
BUFFER_SIZE = 5120
BROADCAST_PORT_CHAT = 58002

BROADCAST_MESSAGE = 'Could I join the Chatroom?'
BROADCAST_ANSWER_SERVER = 'Welcome'

MY_HOST = socket.gethostname()
c_address = socket.gethostbyname(MY_HOST)

# Global variable to save the server address
server_address = None


# Broadcasts that this client is looking for a server
# This shouts into the void until a server is found
def broadcast_sender():
    b_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    # Set the socket to broadcast and enable reusing addresses
    b_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

    b_socket.sendto(str.encode(BROADCAST_MESSAGE), ('<broadcast>', BROADCAST_PORT))
    print(c_address + " send broadcast message")
    print("Searching for server")
    # Wait for a response packet. If no packet has been received in 2 seconds, sleep then broadcast again
    try:
        data, address = b_socket.recvfrom(1024)

        if data and data.startswith(str.encode(BROADCAST_ANSWER_SERVER)):
            set_server_address((address[0], data))
            print(f'Found server at {server_address[0]} with message:', data.decode())

    except TimeoutError:
        broadcast_sender()

    b_socket.close()


# Sets the server address which messages will be sent to
def set_server_address(address: tuple):
    global server_address
    server_address = address


# hier geht der chat los
def handling_messages():
    # wait for user input (Chat-Message) and check whether it's usable
    while True:
        message = input("Your message: \n")
        if not True:
            sys.exit(0)
        if len(message) > BUFFER_SIZE / 10:
            print('Message is too long')
        # send message to server via message_to_server-function
        else:
            message_to_server(message)


# Sends a message to the server
# If the server isn't there, the client starts searching again
def message_to_server(contents):
    # Encode the message using the sender's IP address and the message contents
    message = str(c_address + ': ' + contents)
    message = message.encode('ascii')

    try:
        # Create a TCP socket
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # Connect to the server
        client_socket.connect((server_address[0], TCP_PORT))
        # Set the socket to be non-blocking
        client_socket.setblocking(False)
        # Send the message
        client_socket.send(message)
        # Close the socket
        client_socket.close()

    except(ConnectionRefusedError, TimeoutError):
        # Handle connection errors
        print('\rError - searching for server again')
        broadcast_sender()


def broadcast_listener():
    # Create a socket for listening to broadcast messages
    b_listener = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    # Enable the socket to receive broadcast messages
    b_listener.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    try:
        # Bind the socket to the broadcast port for chat
        b_listener.bind(("", BROADCAST_PORT_CHAT))

    except:
        pass

    while True:
        try:
            # Receive data and address from the broadcast
            data, address = b_listener.recvfrom(BUFFER_SIZE)
        except TimeoutError:
            pass

        else:
            # Filter out messages that were sent by this client
            if ((data.decode()).split(":")[0].split("'")[0]) != c_address:

                if data:
                    # Print the received message and prompt for a new message
                    print("\033[1A\r" + data.decode())
                    print("Your message: ")

    # Close the socket and exit the program
    b_listener.close()
    sys.exit(0)


if __name__ == '__main__':
    # Start the broadcast sender
    broadcast_sender()
    # Create a thread for the broadcast listener
    threadBL = threading.Thread(target=broadcast_listener)
    # Start the broadcast listener thread
    threadBL.start()
    # Create a thread for handling messages
    threadHM = threading.Thread(target=handling_messages)
    # Start the handling messages thread
    threadHM.start()