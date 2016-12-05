/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"

static const int numReplicas = 3;
static const int quorumSize = 2;
// Time after which we stop waiting for the replies for a request, and purge
// the cached request.
static const int requestTimeout = 5;

bool operator!=(Address& addr1, Address& addr2) {
    return !addr1.operator==(addr2);
}

bool operator<(const Message& msg1, const Message& msg2) {
    return msg1.transID < msg2.transID;
}

bool operator==(const Message& msg1, const Message& msg2) {
    return msg1.transID == msg2.transID;
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
}

/**
 * Destructor
 */
MP2Node::~MP2Node() {
	delete ht;
	delete memberNode;
}

/**
 * @brief This method handles the requests which have been queued up beyond the
 *        "requestTimeout" time units.
 */
void MP2Node::handleTimedoutRequests() {
    auto map_it = requestRepliesMap.begin();
    while (map_it != requestRepliesMap.end()) {
        const Message& request = map_it->first;
        int curTs = par->getcurrtime();
        int requestTs = std::get<0>(map_it->second);
        if (curTs - requestTs > requestTimeout) {
            // We didnt receive "quorumSize" number of replies within timeout
            // duration. Fail the request.
            logEvent(request, true, false);
            requestRepliesMap.erase(request);
            map_it = requestRepliesMap.begin();
        } else {
            map_it++;
        }
    }
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
    }

    if (!change) {
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
    }

    // Get a copy of the old "hasMyReplicas"
    vector<Node> oldHasMyReplicas = hasMyReplicas;

    // Initialize the ring based upon this sorted membership list.
    this->ring = curMemList;
    assert(this->ring.size() >= numReplicas);

    // Populate the "haveReplicasOf" and "hasMyReplicas" neighbors.
    findNeighbors();

    if ((ht->currentSize() > 0) && change) {
        stabilizationProtocol(oldHasMyReplicas);
    }

    // Handle the requests which have timed out.
    handleTimedoutRequests();
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
    Message msg(++g_transID, this->memberNode->addr, MessageType::CREATE, key, value);
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
    Message msg(++g_transID, this->memberNode->addr, MessageType::READ, key);
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
    Message msg(++g_transID, this->memberNode->addr, MessageType::UPDATE, key, value);
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
    Message msg(++g_transID, this->memberNode->addr, MessageType::DELETE, key);
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
 * @brief MP2Node::logEvent
 * @param msg
 * @param isCoordinator
 * @param readResult
 * @param result
 */
void MP2Node::logEvent(const Message& msg, bool isCoordinator, bool result,
                       string readResult) {
    switch(msg.type) {
        case MessageType::CREATE:
        {
            if (result) {
                log->logCreateSuccess(&this->memberNode->addr, isCoordinator,
                                      msg.transID, msg.key, msg.value);
            } else {
                log->logCreateFail(&this->memberNode->addr, isCoordinator,
                                   msg.transID, msg.key, msg.value);
            }
         }
         break;
         case MessageType::READ:
         {
            if (result) {
                log->logReadSuccess(&this->memberNode->addr, isCoordinator,
                                    msg.transID, msg.key, readResult);
            } else {
                log->logReadFail(&this->memberNode->addr, isCoordinator, msg.transID, msg.key);
            }
         }
         break;
         case MessageType::UPDATE:
         {
            if (result) {
                log->logUpdateSuccess(&this->memberNode->addr, isCoordinator,
                                    msg.transID, msg.key, msg.value);
            } else {
                log->logUpdateFail(&this->memberNode->addr, isCoordinator,
                                   msg.transID, msg.key, msg.value);
            }
         }
         break;
         case MessageType::DELETE:
            if (result) {
                log->logDeleteSuccess(&this->memberNode->addr, isCoordinator,
                                      msg.transID, msg.key);
            } else {
                log->logDeleteFail(&this->memberNode->addr, isCoordinator,
                                   msg.transID, msg.key);
            }
         break;
         case MessageType::REPLY:
         case MessageType::READREPLY:
             // We should not be logging events for these messages.
             assert(0);
         default:
            printf("Unidentified message received of type::%d.\n", msg.type);
    }
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
               result = createKeyValue(msg.key, msg.value, msg.replica);
               logEvent(msg, false, result);
               Message replyMsg(msg.transID, this->memberNode->addr,
                                MessageType::REPLY, result);
               emulNet->ENsend(&this->memberNode->addr, &msg.fromAddr,
                               replyMsg.toString());
           }
           break;
           case MessageType::READ:
           {
               string value = readKey(msg.key);
               logEvent(msg, false, value == "" ? false : true, value);
               Message replyMsg(msg.transID, this->memberNode->addr, value);
               emulNet->ENsend(&this->memberNode->addr, &msg.fromAddr,
                               replyMsg.toString());
           }
           break;
           case MessageType::UPDATE:
           {
               result = updateKeyValue(msg.key, msg.value, msg.replica);
               logEvent(msg, false, result);
               Message replyMsg(msg.transID, this->memberNode->addr,
                                MessageType::REPLY, result);
               emulNet->ENsend(&this->memberNode->addr, &msg.fromAddr,
                               replyMsg.toString());
           }
           break;
           case MessageType::DELETE:
           {
               result = deletekey(msg.key);
               logEvent(msg, false, result);
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
               if (requestRepliesMap.find(msg) == requestRepliesMap.end()) {
                   // We received a reply for a message which doesnt exist in the
                   // request/reply anymore. This means that we received quorum
                   // number of replies for this request. Ignoring this reply.
               } else {
                   vector<Message>& replies = std::get<1>(requestRepliesMap[msg]);
                   replies.push_back(msg);
               }
           }
           break;
           default:
               printf("Unidentified message received of type::%d.\n", msg.type);
        }
	}

    // Verify that we received successful replies from atleast QUORUM number of replicas.
    auto map_it = requestRepliesMap.begin();
    while (map_it != requestRepliesMap.end()) {
        const Message& request = map_it->first;
        std::vector<Message>& replies = std::get<1>(map_it->second);
        if (replies.size() >= quorumSize) {
            if (request.type == MessageType::READ) {
                // Read the value from one of the READREPLY messages
                assert(replies[0].type == MessageType::READREPLY);
                logEvent(request, true, true, replies[0].value);
                // Remove the request from the map, as we successfully acknowledged it.
                requestRepliesMap.erase(request);
                // Reset the iterator to the beginning.
                map_it = requestRepliesMap.begin();
            } else {
                assert(replies[0].type == MessageType::REPLY);
                // Count the number of successful replies.
                std::vector<Message>::iterator vector_it;
                size_t successReplyCount = 0;
                for (vector_it = replies.begin(); vector_it != replies.end(); ++vector_it) {
                    Message& reply = *vector_it;
                    if (reply.success) {
                        successReplyCount++;
                    }
                }
                if (successReplyCount >= quorumSize) {
                    logEvent(request, true, true);
                    // Remove the request from the map, as we successfully acknowledged it.
                    requestRepliesMap.erase(request);
                    // Reset the iterator to the beginning.
                    map_it = requestRepliesMap.begin();
                } else if (replies.size() == numReplicas) {
                    // We received all the replies for a request from all the replicas
                    // but not "quorumSize" number of successful replies. Marking
                    // the request as failed.
                    logEvent(request, true, false);
                    requestRepliesMap.erase(request);
                    map_it = requestRepliesMap.begin();
                } else {
                    // Move to the next request.
                    map_it++;
                }
            }
        } else {
            // Move to the next request, since we havent received "quorumSize"
            // replies yet.
            map_it++;
        }
    }
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
    if (ring.size() >= numReplicas) {
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
void MP2Node::stabilizationProtocol(vector<Node>& oldHasMyReplicas) {

    // Check if my neighbors have changed. If yes, bring them up to speed with
    // my local key-value store.
    if ((oldHasMyReplicas[0].nodeAddress != hasMyReplicas[0].nodeAddress) ||
         (oldHasMyReplicas[1].nodeAddress != hasMyReplicas[1].nodeAddress)) {
        // One or both of the neighbors changed. Iterate over all the keys in the
        // datastore and dispatch messages for each one.
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
    assert(ring.size() >= numReplicas); // Since each key has 3 replicas.

    // Populate the vector with nodes holding my secondary replicas.
    hasMyReplicas.clear();
    hasMyReplicas.push_back(ring[(i+1) % ring.size()]);
    hasMyReplicas.push_back(ring[(i+2) % ring.size()]);

    // Populate the vector with nodes whose secondary/tertiary replica I am holding.
    haveReplicasOf.clear();
    haveReplicasOf.push_back(ring[(ring.size() + i - 2) % ring.size()]);
    haveReplicasOf.push_back(ring[(ring.size() + i - 1) % ring.size()]);
}


/**
 * This method dispatches the messages to the primay/secondary/tertiary replica of the key.
 * @param message IN: Message to send.
 */

void MP2Node::dispatchMessages(Message message) {
    vector<Node> replicas = findNodes(message.key);
    assert(replicas.size() == numReplicas);

    // Dispatch a message to the PRIMARY replica.
    message.replica = ReplicaType::PRIMARY;
    emulNet->ENsend(&memberNode->addr, &replicas[0].nodeAddress, message.toString());

    // Dispatch a message to the SECONDARY replica.
    message.replica = ReplicaType::SECONDARY;
    emulNet->ENsend(&memberNode->addr, &replicas[1].nodeAddress, message.toString());

    // Dispatch a message to the TERTIARY replica.
    message.replica = ReplicaType::TERTIARY;
    emulNet->ENsend(&memberNode->addr, &replicas[2].nodeAddress, message.toString());

    // Insert an entry for this request.
    auto tuple = std::make_tuple(par->getcurrtime(), std::vector<Message>());
    requestRepliesMap[message] = tuple;
}
