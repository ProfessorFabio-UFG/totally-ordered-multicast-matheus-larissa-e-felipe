from socket import *
import pickle
from constMP import *

port = GROUPMNGR_TCP_PORT
membership = []

def serverLoop():
  serverSock = socket(AF_INET, SOCK_STREAM)
  serverSock.bind(('0.0.0.0', port))
  serverSock.listen(6)
  while(1):
    (conn, addr) = serverSock.accept()
    msgPack = conn.recv(2048)
    req = pickle.loads(msgPack)

    if req["op"] == "register":
      membership.append(req)
      del req["op"]
      print ('Registered peer: ', req)

    elif req["op"] == "list":
      print ('List of peers sent to server: ', membership)
      conn.send(pickle.dumps(membership))

    else:
      pass # fix (send back an answer in case of unknown op

  conn.close()

serverLoop()
