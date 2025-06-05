from constMP import *
from my_socket import *

membership = []

def main():
  serverSock = new_socket("tcp", port=GROUPMNGR_TCP_PORT)

  while True:
    serverSock.accept()
    req = serverSock.read()

    if req["op"] == "register":
      membership.append(req)
      del req["op"]
      print('Registered peer: ', req)

    elif req["op"] == "list":
      print('List of peers sent to server: ', membership)
      serverSock.send(membership)

    else:
      pass # fix (send back an answer in case of unknown op

main()
