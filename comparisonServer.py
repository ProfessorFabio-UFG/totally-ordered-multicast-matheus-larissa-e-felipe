from constMP import *
from my_socket import *

serverSock = new_socket("tcp", port=COMPARISON_SERVER_PORT)
grp_mng_addr = (GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT)

def main():
	while True:
		nMsgs = promptUser()

		if nMsgs == 0:
			break

		clientSock = MySocket("tcp")
		clientSock.connect(grp_mng_addr)
		req = {"op": "list"}
		clientSock.send(req)
		peerList = clientSock.read()
		clientSock.close()
		print("List of Peers: ", peerList)
		startPeers(peerList, nMsgs)
		print('Now, wait for the message logs from the communicating peers...')
		waitForLogsAndCompare(nMsgs, len(peerList))

	serverSock.close()

def promptUser():
	nMsgs = int(input('Enter the number of messages for each peer to send (0 to terminate)=> '))
	return nMsgs

def startPeers(peerList, nMsgs):
	# Connect to each of the peers and send the 'initiate' signal:
	peerNumber = 0
	for peer in peerList:
		clientSock = MySocket("tcp")
		clientSock.connect((peer["ipaddr"], peer["tcp_port"]))
		msg = {
			'id': peerNumber,
			'nMsgs': nMsgs
		}
		clientSock.send(msg)
		msg = clientSock.read()
		print(f"Peer process {msg['id']} started.")
		clientSock.close()
		peerNumber = peerNumber + 1

def waitForLogsAndCompare(N_MSGS, number_of_peers):
	# Loop to wait for the message logs for comparison:
	numPeers = 0
	msgs = [] # each msg is a list of tuples (with the original messages received by the peer processes)

	# Receive the logs of messages from the peer processes
	while numPeers < number_of_peers:
		serverSock.accept()
		msg = serverSock.read(32768)
		print ('Received log from peer')
		serverSock.close()
		msgs.append(msg)
		numPeers = numPeers + 1

	unordered = 0

	# Compare the lists of messages
	for j in range(0, N_MSGS - 1):
		firstMsg = msgs[0][j]
		for i in range(1, number_of_peers-1):
			if firstMsg != msgs[i][j]:
				unordered = unordered + 1
				break
	
	print ('Found ' + str(unordered) + ' unordered message rounds')


# Initiate server:
main()
