from constMP import *
from my_socket import *
import threading
import random
import time
from requests import get

#handShakes = [] # not used; only if we need to check whose handshake is missing

handShakeCount = 0 # Counter to make sure we have received handshakes from all other processes
clock = 0  # Process logic clock
addresses_to_send = [] # Other peers
my_id = None
grp_mng_addr = (GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT)
comp_server_addr = (COMPARISON_SERVER_ADDR, COMPARISON_SERVER_PORT)


def get_public_ip():
  ipAddr = get('https://api.ipify.org').content.decode('utf8')
  print('My public IP address is: {}'.format(ipAddr))
  return ipAddr

# Function to register this peer with the group manager
def registerWithGroupManager(tcp_socket, udp_socket):
  clientSock = MySocket("tcp")
  print('Connecting to group manager: ', grp_mng_addr)
  clientSock.connect(grp_mng_addr)
  ipaddr = get_public_ip() if not DEV_MODE else "127.0.0.1"

  req = {
    "op": "register",
    "ipaddr": ipaddr,
    "tcp_port": tcp_socket.get_port(),
    "udp_port": udp_socket.get_port(),
  }

  print ('Registering with group manager')
  clientSock.send(req)
  clientSock.close()

def getListOfPeers():
  clientSock = MySocket('tcp')
  print ('Connecting to group manager: ', grp_mng_addr)
  clientSock.connect(grp_mng_addr)
  req = {"op": "list"}
  clientSock.send(req)
  peers = clientSock.read(2048)
  print ('Got list of peers: ', peers)
  clientSock.close()
  return peers

class MsgHandler(threading.Thread):
  def __init__(self, sock):
    threading.Thread.__init__(self)
    self.sock = sock

  def run(self):
    print('Handler is ready. Waiting for the handshakes...')

    global handShakeCount
    global clock
    global addresses_to_send
    global my_id
    logList = []

    # UDP sockets to send and receive data messages:
    # Create send socket
    sendSocket = MySocket("udp")
    
    # Wait until handshakes are received from all other processes
    # (to make sure that all processes are synchronized before they start exchanging messages)
    while handShakeCount < len(addresses_to_send):
      msg = self.sock.read()
      #print ('########## unpickled msgPack: ', msg)

      if msg['op'] == 'READY':

        # To do: send reply of handshake and wait for confirmation

        handShakeCount = handShakeCount + 1
        #handShakes[msg[1]] = 1

    print('Received all handshakes. Entering the loop to receive messages.')

    stopCount=0 
    while True:
      if stopCount == len(addresses_to_send):
        break  # stop loop when all other processes have finished

      received_message = self.sock.read()
      clock += 1

      #print(received_message)

      if received_message['op'] == 'END':   # count the 'stop' messages from the other processes
        stopCount += 1
        continue

      #if received_message['op'] == 'ACK':
        #print("ACK received")

      if received_message['op'] == 'DATA':
        logList.append(received_message)
        ack_message = {
          'op': 'ACK',
          'source_id': received_message['source_id'],
          'message_id': received_message['message_id'],
          'destination_id': my_id,
          'time_stamp': clock
        }

        for addr in addresses_to_send:
          sendSocket.send_to(ack_message, addr)
        
    # Write log file
    logFile = open('logfile'+str(my_id)+'.log', 'w')
    logFile.writelines(str(logList))
    logFile.close()
    
    # Send the list of messages to the server (using a TCP socket) for comparison
    print('Sending the list of messages to the server for comparison...')
    clientSock = MySocket("tcp")
    clientSock.connect(comp_server_addr)
    clientSock.send(logList)
    clientSock.close()
    
    # Reset the handshake counter
    handShakeCount = 0

    exit(0)

# Function to wait for start signal from comparison server:
def waitToStart(serverSock):
  serverSock.accept()
  msg = serverSock.read()
  serverSock.send(msg)
  serverSock.close()
  return (msg['id'], msg['nMsgs'])

def peer_to_addr(peer):
  return (peer["ipaddr"], peer["udp_port"])

def main():
  global handShakeCount
  global clock
  global addresses_to_send
  global my_id

  # TCP socket to receive start signal from the comparison server:
  tcp_socket = new_socket("tcp")

  # Create and bind receive socket
  udp_socket = new_socket("udp")

  registerWithGroupManager(tcp_socket, udp_socket)

  while True:
    print('Waiting for signal to start...')
    (my_id, nMsgs) = waitToStart(tcp_socket)
    print('I am up, and my ID is: ', str(my_id))

    if nMsgs == 0:
      print('Terminating.')
      exit(0)

    # Wait for other processes to be ready
    # To Do: fix bug that causes a failure when not all processes are started within this time
    # (fully started processes start sending data messages, which the others try to interpret as control messages) 
    time.sleep(5)

    peers = getListOfPeers()

    for peer in peers:
      addresses_to_send.append(peer_to_addr(peer))

    # Create receiving message handler
    MsgHandler(udp_socket).start()

    # UDP sockets to send and receive data messages:
    # Create send socket
    sendSocket = MySocket("udp")
    
    # Send handshakes
    # To do: Must continue sending until it gets a reply from each process
    #        Send confirmation of reply
    for addr in addresses_to_send:
      print('Sending handshake to ', addr)
      msg = {
        'op': 'READY',
        'source_id':  my_id
      }
      sendSocket.send_to(msg, addr)
      #data = recvSocket.recvfrom(128) # Handshadke confirmations have not yet been implemented

    print('Sent all handshakes. handShakeCount=', str(handShakeCount))

    while (handShakeCount < len(peers)):
      pass  # find a better way to wait for the handshakes

    # Send a sequence of data messages to all other processes 
    for msgNumber in range(0, nMsgs):
      # Wait some random time between successive messages
      time.sleep(random.randrange(10, 100) / 1000)

      msg = {
        'op': 'DATA',
        'source_id': my_id,
        'message_id': msgNumber,
        'time_stamp': clock
      }
      clock += 1

      for addr in addresses_to_send:
        sendSocket.send_to(msg, addr)

    # Tell all processes that I have no more messages to send
    for addr in addresses_to_send:
      msg = {'op': 'END'}
      sendSocket.send_to(msg, addr)

    print(f"MY CLOCK {clock}")

main()