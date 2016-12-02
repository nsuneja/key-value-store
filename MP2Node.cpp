/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"

bool operator!=(Address& addr1, Address& addr2) {
    return !addr1.operator==(addr2);
}

bool operator==(const Node& node1, const Node& node2) {
    return node1.nodeAddress == node2.nodeAddress;
}

bool operator!=(const Node& node1, const Node& node2) {
    return !operator==(node1, node2);
}


/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet * emulNet, Log * log, Address * address) {
	this->memberNode = memberNode;
	this->par = par;
	this->emulNet = emulNet;
	this->log = log;
	ht = new HashTable();
	this->memberNode->addr = *address;
    this->curTransId = 1;
}

/**
 * Destructor
 */
MP2Node::~MP2Node() {
	delete ht;
	delete memberNode;
}

/**
 * FUNCTION NAME: updateRing
 *
 * DESCRIPTION: This function does the following:
 * 				1) Gets the current membership list from the Membership Protocol (MP1Node)
 * 				   The membership list is returned as a vector of Nodes. See Node class in Node.h
 * 				2) Constructs the ring based on the membership list
 * 				3) Calls the Stabilization Protocol
 */
void MP2Node::updateRing() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	vector<Node> curMemList;
	bool change = false;

	/*
	 *  Step 1. Get the current membership list from Membership Protocol / MP1
	 */
	curMemList = getMembershipList();

	/*
	 * Step 2: Construct the ring
	 */
	// Sort the list based on the hashCode
	sort(curMemList.begin(), curMemList.end());

	/*
	 * Step 3: Run the stabilization protocol IF REQUIRED
	 */
    // Run stabilization protocol if the hash table size is greater than zero and if there has been a changed in the ring
    if (curMemList.size() != this->ring.size()) { // if the ring size change.
        change = true;
        goto stabilize;
    }

    // Check if any member node in the ring changed.
    std::vector<Node>::iterator it1, it2;
    for (it1 = curMemList.begin(), it2 = this->ring.begin(); it1 != curMemList.end(); ++it1, ++it2) {
        Node& node1 = *it1;
        Node& node2 = *it2;
        if (node1.nodeAddress != node2.nodeAddress) {
            change = true;
            break;
        }
    }

stabilize:
    // Initialize the ring based upon this sorted membership list.
    this->ring = curMemList;

    if ((ht->currentSize() > 0) && change) {
        stabilizationProtocol();
    }
}

/**
 * FUNCTION NAME: getMemberhipList
 *
 * DESCRIPTION: This function goes through the membership list from the Membership protocol/MP1 and
 * 				i) generates the hash code for each member
 * 				ii) populates the ring member in MP2Node class
 * 				It returns a vector of Nodes. Each element in the vector contain the following fields:
 * 				a) Address of the node
 * 				b) Hash code obtained by consistent hashing of the Address
 */
vector<Node> MP2Node::getMembershipList() {
	unsigned int i;
	vector<Node> curMemList;
	for ( i = 0 ; i < this->memberNode->memberList.size(); i++ ) {
		Address addressOfThisMember;
		int id = this->memberNode->memberList.at(i).getid();
		short port = this->memberNode->memberList.at(i).getport();
		memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
		memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));
		curMemList.emplace_back(Node(addressOfThisMember));
	}
	return curMemList;
}

/**
 * FUNCTION NAME: hashFunction
 *
 * DESCRIPTION: This functions hashes the key and returns the position on the ring
 * 				HASH FUNCTION USED FOR CONSISTENT HASHING
 *
 * RETURNS:
 * size_t position on the ring
 */
size_t MP2Node::hashFunction(string key) {
	std::hash<string> hashFunc;
	size_t ret = hashFunc(key);
	return ret%RING_SIZE;
}

/**
 * FUNCTION NAME: clientCreate
 *
 * DESCRIPTION: client side CREATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientCreate(string key, string value) {
    Message msg(curTransId++, this->memberNode->addr, MessageType::CREATE, key, value);
    (void)createKeyValue(key, value, ReplicaType::PRIMARY);
    findNeighbors();
    dispatchMessages(msg);
}

/**
 * FUNCTION NAME: clientRead
 *
 * DESCRIPTION: client side READ API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientRead(string key) {
    Message msg(curTransId++, this->memberNode->addr, MessageType::READ, key);
    findNeighbors();
    dispatchMessages(msg);

}

/**
 * FUNCTION NAME: clientUpdate
 *
 * DESCRIPTION: client side UPDATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientUpdate(string key, string value){
    Message msg(curTransId++, this->memberNode->addr, MessageType::UPDATE, key);
    updateKeyValue(key, value, ReplicaType::PRIMARY);
    findNeighbors();
    dispatchMessages(msg);
}

/**
 * FUNCTION NAME: clientDelete
 *
 * DESCRIPTION: client side DELETE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientDelete(string key){
    Message msg(curTransId++, this->memberNode->addr, MessageType::DELETE, key);
    deletekey(key);
    findNeighbors();
    dispatchMessages(msg);
}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(string key, string value, ReplicaType replica) {
    return ht->create(key, value);
}

/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 * 			    This function does the following:
 * 			    1) Read key from local hash table
 * 			    2) Return value
 */
string MP2Node::readKey(string key) {
    return ht->read(key);
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica) {
    return ht->update(key, value);
}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 * 				This function does the following:
 * 				1) Delete the key from the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::deletekey(string key) {
    return ht->deleteKey(key);
}


/**
 * @brief This method verifies the source of the message by checking that the
 * message originates from a replica whose SECONDARY or TERTIARY replica we have.
 * @param msg IN: Received message.
 */
void
MP2Node::verifyReceivedMsgSource(Message& msg)
{
    // Verify that I should have a secondary or tertiary copy of the
    // replica.
    assert(haveReplicasOf.size() == 2);
    assert(haveReplicasOf[0].nodeAddress == msg.fromAddr ||
           haveReplicasOf[1].nodeAddress == msg.fromAddr);
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: This function is the message handler of this node.
 * 				This function does the following:
 * 				1) Pops messages from the queue
 * 				2) Handles the messages according to message types
 */
void MP2Node::checkMessages() {
	char * data;
	int size;
    bool result;

	// dequeue all messages and handle them
	while ( !memberNode->mp2q.empty() ) {
		/*
		 * Pop a message from the queue
		 */
		data = (char *)memberNode->mp2q.front().elt;
		size = memberNode->mp2q.front().size;
		memberNode->mp2q.pop();

		string message(data, data + size);
        Message msg(message);

        switch (msg.type) {
           case MessageType::CREATE:
           {
               verifyReceivedMsgSource(msg);
               result = createKeyValue(msg.key, msg.value, msg.replica);
               Message replyMsg(msg.transID, this->memberNode->addr,
                                MessageType::REPLY, result);
               emulNet->ENsend(&this->memberNode->addr, &msg.fromAddr,
                               replyMsg.toString());
           }
           break;
           case MessageType::READ:
           {
               verifyReceivedMsgSource(msg);
               string value(readKey(msg.key));
               Message replyMsg(msg.transID, this->memberNode->addr, value);
               emulNet->ENsend(&this->memberNode->addr, &msg.fromAddr,
                               replyMsg.toString());
           }
           break;
           case MessageType::UPDATE:
           {
               verifyReceivedMsgSource(msg);
               result = updateKeyValue(msg.key, msg.value, msg.replica);
               Message replyMsg(msg.transID, this->memberNode->addr,
                                MessageType::REPLY, result);
               emulNet->ENsend(&this->memberNode->addr, &msg.fromAddr,
                               replyMsg.toString());
           }
           break;
           case MessageType::DELETE:
           {
               verifyReceivedMsgSource(msg);
               result = deletekey(msg.key);
               Message replyMsg(msg.transID, this->memberNode->addr,
                                MessageType::REPLY, result);
               emulNet->ENsend(&this->memberNode->addr, &msg.fromAddr,
                               replyMsg.toString());
           }
           break;
           case MessageType::REPLY:
           case MessageType::READREPLY:
           {
               // Bump up the reply count.
               assert(replyCount.find(msg.transID) != replyCount.end());
               replyCount[msg.transID]++;
           }
           break;
           default:
               printf("Unidentified message received of type::%d.\n", msg.type);
        }
	}

    // Verify that we received replies from all the replicas of the key.
    std::map<uint64_t, size_t>::iterator it;
    for (it = replyCount.begin(); it != replyCount.end(); ++it) {
        // Verify that we received reply from SECONDARY AND TERTIARY copy of the replica.
        assert(it->second == 2);
    }
    replyCount.clear();
}

/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given keyfunction
 * 				This function is responsible for finding the replicas of a key
 */
vector<Node> MP2Node::findNodes(string key) {
	size_t pos = hashFunction(key);
	vector<Node> addr_vec;
	if (ring.size() >= 3) {
		// if pos <= min || pos > max, the leader is the min
		if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size()-1).getHashCode()) {
			addr_vec.emplace_back(ring.at(0));
			addr_vec.emplace_back(ring.at(1));
			addr_vec.emplace_back(ring.at(2));
		}
		else {
			// go through the ring until pos <= node
			for (int i=1; i<ring.size(); i++){
				Node addr = ring.at(i);
				if (pos <= addr.getHashCode()) {
					addr_vec.emplace_back(addr);
					addr_vec.emplace_back(ring.at((i+1)%ring.size()));
					addr_vec.emplace_back(ring.at((i+2)%ring.size()));
					break;
				}
			}
		}
	}
	return addr_vec;
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: Receive messages from EmulNet and push into the queue (mp2q)
 */
bool MP2Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1, &(memberNode->mp2q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}
/**
 * FUNCTION NAME: stabilizationProtocol
 *
 * DESCRIPTION: This runs the stabilization protocol in case of Node joins and leaves
 * 				It ensures that there always 3 copies of all keys in the DHT at all times
 * 				The function does the following:
 *				1) Ensures that there are three "CORRECT" replicas of all the keys in spite of failures and joins
 *				Note:- "CORRECT" replicas implies that every key is replicated in its two neighboring nodes in the ring
 */
void MP2Node::stabilizationProtocol() {
    vector<Node> oldHasMyReplicas = hasMyReplicas;

    // Obtain the new has "hasMyReplicas".
    findNeighbors();

    // Check if my neighbors have changed. If yes, bring them up to speed with
    // my local key-value store.
    if ((oldHasMyReplicas[0] != hasMyReplicas[0]) || (oldHasMyReplicas[1] != hasMyReplicas[1])) {
        // One of the neighbors changed. Iterate over all the keys in the datastore
        // and dispatch messages for each one.
        std::map<string, string>::iterator it;
        for (it = ht->hashTable.begin(); it != ht->hashTable.end(); ++it) {
            clientCreate(it->first, it->second);
        }
    }
}


/**
 * This method finds its own position on the ring, and populates the sucessors
 * and predecessor neighbors.
 */

void MP2Node::findNeighbors() {
    // Iterate through the ring and locate myself on it.
    int i;
    for (i = 0; i < ring.size(); i++) {
        Node& node = ring[i];
        if (node.nodeAddress == this->memberNode->addr) {
            // Found ourselves!
            break;
        }
    }

    assert(i != ring.size()); // Ensure that I always find myself in the ring.
    assert(ring.size() >= 3); // Since each key has 3 replicas.

    // Populate the vector with nodes holding my secondary replicas.
    hasMyReplicas.clear();
    hasMyReplicas.push_back(ring[(i+1) % ring.size()]);
    hasMyReplicas.push_back(ring[(i+2) % ring.size()]);

    // Populate the vector with nodes whose secondary replica I am holding.
    haveReplicasOf.clear();
    haveReplicasOf.push_back(ring[(i-2) % ring.size()]);
    haveReplicasOf.push_back(ring[(i-1) % ring.size()]);
}


/**
 * This method dispatches the messages to the secondary/tertiary replica of the key.
 * @param message IN: Message to send.
 */

void MP2Node::dispatchMessages(Message message) {
    assert(hasMyReplicas.size() == 2);

    // Dispatch a message to the SECONDARY replica.
    message.replica = ReplicaType::SECONDARY;
    emulNet->ENsend(&memberNode->addr, &hasMyReplicas[0].nodeAddress, message.toString());

    // Dispatch a message to the TERTIARY replica.
    message.replica = ReplicaType::TERTIARY;
    emulNet->ENsend(&memberNode->addr, &hasMyReplicas[1].nodeAddress, message.toString());

    // Set the reply counter to zero.
    replyCount[message.transID] = 0;
}
