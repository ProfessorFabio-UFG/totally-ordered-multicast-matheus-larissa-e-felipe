from socket  import *
from constMP import * #-
import threading
import random
import time
import pickle
from requests import get

#handShakes = [] # not used; only if we need to check whose handshake is missing

# Counter to make sure we have received handshakes from all other processes
handShakeCount = 0

def get_socket(protocol):
  if protocol == "tcp":
    p = SOCK_STREAM
  elif protocol == "udp":
    p = SOCK_DGRAM
  else:
    raise Exception("Protocol must be tcp or udp")

  s = socket(AF_INET, p)
  # Bind to port 0 to let the OS assign an available port
  s.bind(('127.0.0.1', 0))
  return s

def get_public_ip():
  ipAddr = get('https://api.ipify.org').content.decode('utf8')
  print('My public IP address is: {}'.format(ipAddr))
  return ipAddr

# Function to register this peer with the group manager
def registerWithGroupManager(tcp_socket, udp_socket):
  clientSock = socket(AF_INET, SOCK_STREAM)
  print('Connecting to group manager: ', (GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT))
  clientSock.connect((GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT))
  req = {
    "op": "register",
    "ipaddr": get_public_ip(),
    "tcp_port": tcp_socket.getsockname()[1],
    "udp_port": udp_socket.getsockname()[1],
  }
  msg = pickle.dumps(req)
  print ('Registering with group manager: ', req)
  clientSock.send(msg)
  clientSock.close()

def getListOfPeers():
  clientSock = socket(AF_INET, SOCK_STREAM)
  print ('Connecting to group manager: ', (GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
  clientSock.connect((GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
  req = {"op":"list"}
  msg = pickle.dumps(req)
  clientSock.send(msg)
  msg = clientSock.recv(2048)
  peers = pickle.loads(msg)
  print ('Got list of peers: ', peers)
  clientSock.close()
  return peers

class MsgHandler(threading.Thread):
  def __init__(self, sock, my_self, number_of_peers):
    threading.Thread.__init__(self)
    self.sock = sock
    self.my_self = my_self
    self.number_of_peers = number_of_peers

  def run(self):
    print('Handler is ready. Waiting for the handshakes...')
    
    #global handShakes
    global handShakeCount
    
    logList = []
    
    # Wait until handshakes are received from all other processes
    # (to make sure that all processes are synchronized before they start exchanging messages)
    while handShakeCount < self.number_of_peers:
      msgPack = self.sock.recv(1024)
      msg = pickle.loads(msgPack)
      #print ('########## unpickled msgPack: ', msg)
      if msg[0] == 'READY':

        # To do: send reply of handshake and wait for confirmation

        handShakeCount = handShakeCount + 1
        #handShakes[msg[1]] = 1
        print('--- Handshake received: ', msg[1])

    print('Secondary Thread: Received all handshakes. Entering the loop to receive messages.')

    stopCount=0 
    while True:                
      msgPack = self.sock.recv(1024)   # receive data from client
      msg = pickle.loads(msgPack)
      if msg[0] == -1:   # count the 'stop' messages from the other processes
        stopCount = stopCount + 1
        if stopCount == self.number_of_peers:
          break  # stop loop when all other processes have finished
      else:
        print('Message ' + str(msg[1]) + ' from process ' + str(msg[0]))
        logList.append(msg)
        
    # Write log file
    logFile = open('logfile'+str(self.my_self)+'.log', 'w')
    logFile.writelines(str(logList))
    logFile.close()
    
    # Send the list of messages to the server (using a TCP socket) for comparison
    print('Sending the list of messages to the server for comparison...')
    clientSock = socket(AF_INET, SOCK_STREAM)
    clientSock.connect((SERVER_ADDR, SERVER_PORT))
    msgPack = pickle.dumps(logList)
    clientSock.send(msgPack)
    clientSock.close()
    
    # Reset the handshake counter
    handShakeCount = 0

    exit(0)

# Function to wait for start signal from comparison server:
def waitToStart(serverSock):
  (conn, addr) = serverSock.accept()
  msgPack = conn.recv(1024)
  msg = pickle.loads(msgPack)
  myself = msg[0]
  nMsgs = msg[1]
  conn.send(pickle.dumps('Peer process '+str(myself)+' started.'))
  conn.close()
  return (myself,nMsgs)

def peer_to_addr(peer):
  return (peer["ipaddr"], peer["udp_port"])

def main():
  # TCP socket to receive start signal from the comparison server:
  tcp_socket = get_socket("tcp")
  tcp_socket.listen(1)

  # Create and bind receive socket
  udp_socket = get_socket("udp")

  registerWithGroupManager(tcp_socket, udp_socket)

  while True:
    print('Waiting for signal to start...')
    (my_self, nMsgs) = waitToStart(tcp_socket)
    print('I am up, and my ID is: ', str(my_self))

    if nMsgs == 0:
      print('Terminating.')
      exit(0)

    # Wait for other processes to be ready
    # To Do: fix bug that causes a failure when not all processes are started within this time
    # (fully started processes start sending data messages, which the others try to interpret as control messages) 
    time.sleep(5)

    peers = getListOfPeers()

    # Create receiving message handler
    msgHandler = MsgHandler(udp_socket, my_self, len(peers))
    msgHandler.start()
    print('Handler started')

    if DEV_MODE:
      for i in range(len(peers)):
        peers[i]["ipaddr"] = "127.0.0.1"

    addresses_to_send = []
    for peer in peers:
      addresses_to_send.append(peer_to_addr(peer))

    # UDP sockets to send and receive data messages:
    # Create send socket
    sendSocket = socket(AF_INET, SOCK_DGRAM)
    
    # Send handshakes
    # To do: Must continue sending until it gets a reply from each process
    #        Send confirmation of reply
    for addr in addresses_to_send:
      print('Sending handshake to ', addr)
      msg = ('READY', my_self)
      msgPack = pickle.dumps(msg)
      sendSocket.sendto(msgPack, addr)
      #data = recvSocket.recvfrom(128) # Handshadke confirmations have not yet been implemented

    print('Main Thread: Sent all handshakes. handShakeCount=', str(handShakeCount))

    while (handShakeCount < len(peers)):
      pass  # find a better way to wait for the handshakes

    # Send a sequence of data messages to all other processes 
    for msgNumber in range(0, nMsgs):
      # Wait some random time between successive messages
      time.sleep(random.randrange(10, 100) / 1000)
      msg = (my_self, msgNumber)
      msgPack = pickle.dumps(msg)

      for addr in addresses_to_send:
        sendSocket.sendto(msgPack, addr)
        print('Sent message ' + str(msgNumber))

    # Tell all processes that I have no more messages to send
    for addr in addresses_to_send:
      msg = (-1,-1)
      msgPack = pickle.dumps(msg)
      sendSocket.sendto(msgPack, addr)

main()