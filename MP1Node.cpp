/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"
#include <ctime>

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */


static int getid(Address& addr) {
   int id;
   memcpy(&id, &addr.addr[0], sizeof(int));
   return id;
}

static short getport(Address& addr) {
    short port;
    memcpy(&port, &addr.addr[4], sizeof(short));
    return port;
}

static void getAddressFromEntry(MemberListEntry& entry, Address& addr) {
    memcpy(&addr.addr[0], &entry.id, sizeof(int));
    memcpy(&addr.addr[4], &entry.port, sizeof(short));
}


bool operator==(const MemberListEntry& entry1, const MemberListEntry &entry2) {
    return (entry1.id == entry2.id) && (entry1.port == entry2.port);
}


/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
    this->timedoutMap.clear();
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}


int64_t MP1Node::getTimedoutCounter(Address *addr)
{
    string addrStr = addr->getAddress();
    if (timedoutMap.count(addrStr) == 0) {
        timedoutMap[addrStr] = -1;
        return timedoutMap[addrStr];
    }
    return timedoutMap[addrStr];
}


void MP1Node::setTimedoutCounter(Address* addr, int64_t counter)
{
    string addrStr = addr->getAddress();
    assert(timedoutMap.count(addrStr) > 0);
    assert(timedoutMap[addrStr] == -1);
    timedoutMap[addrStr] = counter;
}


void MP1Node::decrementTimedoutCounter(Address* addr)
{
    string addrStr = addr->getAddress();
    assert(timedoutMap.count(addrStr) > 0);
    assert(timedoutMap[addrStr] > 0);
    timedoutMap[addrStr] = timedoutMap[addrStr] - 1;
}


void MP1Node::removeTimedoutCounter(Address* addr)
{
    string addrStr = addr->getAddress();
    assert(timedoutMap.count(addrStr) > 0);
    assert(timedoutMap[addrStr] == 0);
    timedoutMap.erase(addrStr);
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */
	int id = *(int*)(&memberNode->addr.addr);
	int port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
    memberNode->heartbeat = 0;
    memberNode->pingCounter = TFAIL;
    memberNode->timeOutCounter = TREMOVE;
    initMemberListTable(memberNode);

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    }
    else {
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);

        free(msg);
    }

    // Add ourselves to the memberlist.
    MemberListEntry mem(getid(memberNode->addr), getport(memberNode->addr),
                        memberNode->heartbeat, this->par->getcurrtime());
    memberNode->memberList.push_back(mem);
    memberNode->myPos = std::find(memberNode->memberList.begin(),
                                  memberNode->memberList.end(),
                                  mem);
    assert(memberNode->myPos != memberNode->memberList.end());

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode() {
    this->memberNode->inGroup = false;
    this->memberNode->bFailed = true;
    this->memberNode->nnb = 0;
    this->memberNode->memberList.clear();
    this->timedoutMap.clear();
    return 0;
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
    Member* me;
    MessageHdr* msg;
    Address* senderAddress;
    long senderHearbeatCounter;
    size_t msgsize, memListSize;
    MemberListEntry* memListEntry;
    uint32_t numMembers;
    Address memberAddress;
    vector<MemberListEntry>::iterator iter;

    me = static_cast<Member*>(env);
    assert(memcmp(static_cast<void*>(me), static_cast<void*>(memberNode),
                                                   sizeof(Member)) == 0);
    msg = reinterpret_cast<MessageHdr*>(data);

    switch(msg->msgType) {
        case MsgTypes::JOINREQ:
        {
            senderAddress = reinterpret_cast<Address*>(msg + 1);
            senderHearbeatCounter = *reinterpret_cast<long*>((char *)(msg+1) + 1
                                           + sizeof(senderAddress->addr));
            msgsize = sizeof(MessageHdr) + sizeof(senderAddress->addr)
                                           + sizeof(long) + 1;
            assert(msgsize == size);

            // Send the current list of members to the sender node.
            memListSize = me->memberList.size() * sizeof(MemberListEntry);
            msgsize = sizeof(MessageHdr) + sizeof(memberNode->addr.addr) + memListSize;
            msg = static_cast<MessageHdr*>(malloc(msgsize * sizeof(char)));

            msg->msgType = MsgTypes::JOINREP;
            memcpy(static_cast<void*>(msg + 1),
                   static_cast<void*>(memberNode->addr.addr),
                   sizeof(memberNode->addr.addr));
            memcpy(static_cast<void*>((char*)(msg+1) + sizeof(memberNode->addr.addr)),
                   static_cast<void*>(me->memberList.data()),
                   memListSize);
            emulNet->ENsend(&memberNode->addr, senderAddress,
                            reinterpret_cast<char*>(msg), msgsize);

            // Free the memory allocated towards the message.
            free(msg);

            // Create an entry for the sender in our memberid list.
            MemberListEntry mem(getid(*senderAddress), getport(*senderAddress),
                                senderHearbeatCounter, this->par->getcurrtime());
            if (std::find(me->memberList.begin(), me->memberList.end(), mem) ==
                                                 me->memberList.end()) {
                // Set the local timestamp.
                mem.setheartbeat(me->heartbeat);
                me->memberList.push_back(mem);
                me->nnb++;
                this->log->logNodeAdd(&me->addr, senderAddress);
            } else {
                this->log->LOG(&me->addr, "Duplicate JOIN request. Node::%s"
                                         " already part of the cluster.",
                                         senderAddress->getAddress().c_str ());
            }
        }
        break;
        case MsgTypes::JOINREP:
        {
            // Populate my member list.
            senderAddress = reinterpret_cast<Address*>(msg + 1);
            memListEntry = reinterpret_cast<MemberListEntry*>((char *)(msg+1) +
                                                  sizeof(senderAddress->addr));
            numMembers = (size - sizeof(MessageHdr))/sizeof(MemberListEntry);
            for (int i = 0; i < numMembers; i++, memListEntry++) {
                getAddressFromEntry(*memListEntry, memberAddress);
                // Set the local timestamp.
                memListEntry->setheartbeat(me->heartbeat);
                me->memberList.push_back(*memListEntry);
                // Log a node add event.
                me->nnb++;
                this->log->logNodeAdd(&me->addr, &memberAddress);
            }

            // Mark ourself joined.
            me->inGroup = true;
        }
        break;
        case MsgTypes::MEMSHIP:
        {
            memListEntry = reinterpret_cast<MemberListEntry*>(msg + 1);
            numMembers = (size - sizeof(MessageHdr))/sizeof(MemberListEntry);
            for (int i = 0; i < numMembers; i++, memListEntry++) {
                iter = std::find(me->memberList.begin(), me->memberList.end(),
                                                               *memListEntry);
                // Fetch the member address.
                getAddressFromEntry(*memListEntry, memberAddress);
                // Only add the entry if it's a "new" heartbeat, and not a
                // "stale" one.
                if ((iter == me->memberList.end()) &&
                        ((this->par->getcurrtime() - memListEntry->getheartbeat())
                          < this->memberNode->timeOutCounter)) {
                    // Set the local timestamp and add an entry for the
                    // corresponding node to the table
                    memListEntry->setheartbeat(me->heartbeat);
                    me->memberList.push_back(*memListEntry);
                    me->nnb++;
                    this->log->logNodeAdd(&me->addr, &memberAddress);
                } else {
                    // Ignore membership updates for the entries for which a
                    // timedout counter is running.
                    if ((getTimedoutCounter(&memberAddress) == -1) &&
                           (memListEntry->gettimestamp() > iter->gettimestamp())) {
                        // Update the global timestamp.
                        iter->settimestamp(memListEntry->gettimestamp());
                        // Update the local hearbeat.
                        iter->setheartbeat(me->heartbeat);
                    }
                }
            }
        }
        break;
        default:
            printf("Unidentified message received of type::%d.\n", msg->msgType);
    }
    return true;
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {
    Address entryAddr;
    size_t msgsize, memListSize;
    MessageHdr* msg;
    long heartbeatDiff;
    MemberListEntry memListEntry;

    // Iterate through our membership table and check if an entry has not been
    // updated in T_REMOVE hearbeats.
    vector<MemberListEntry>::iterator iter = this->memberNode->memberList.begin();
    while (iter != this->memberNode->memberList.end()) {
        // Fetch the address of the entry.
        getAddressFromEntry(*iter, entryAddr);

        heartbeatDiff = this->memberNode->heartbeat - iter->getheartbeat();
        if ((heartbeatDiff > this->memberNode->timeOutCounter) &&
                            (getTimedoutCounter(&entryAddr) == -1)) {
            // The member has not responded in T_REMOVE hearbeats. Starting
            // the timeout counter.
            setTimedoutCounter(&entryAddr, this->memberNode->timeOutCounter);
            iter++;
        } else if (getTimedoutCounter(&entryAddr) > 0) {
            // Timedout counter already running for this entry.
            decrementTimedoutCounter(&entryAddr);
            iter++;
        } else if (getTimedoutCounter(&entryAddr) == 0) {
            // Timedout counter expired for this entry. Remove the node.
            // Ensure that we don't remove our entry from the membership table.
            if (memcmp(&this->memberNode->addr, &entryAddr, sizeof(Address)) != 0) {
                iter = this->memberNode->memberList.erase(iter);
                this->log->logNodeRemove(&this->memberNode->addr, &entryAddr);
                this->memberNode->nnb--;
                // Remove the entry from the map.
                removeTimedoutCounter(&entryAddr);
            } else {
                iter++;
            }
        } else {
            iter++;
        }

        if (this->memberNode->pingCounter == 0) {

            // Update my entry in the membership list with the latest global
            // timestamp and current local heartbeat counter, before sending
            // the membership list.
            memListEntry = MemberListEntry(getid(this->memberNode->addr),
                                           getport(this->memberNode->addr));
            if (memcmp(&entryAddr, &this->memberNode->addr, sizeof(Address)) == 0) {
                memberNode->myPos = std::find(memberNode->memberList.begin(),
                                              memberNode->memberList.end(),
                                              memListEntry);
                assert(memberNode->myPos != memberNode->memberList.end());
                this->memberNode->myPos->settimestamp(this->par->getcurrtime());
                this->memberNode->myPos->setheartbeat(this->memberNode->heartbeat);
            }

            memListSize = this->memberNode->memberList.size() * sizeof(MemberListEntry);
            msgsize = sizeof(MessageHdr) + memListSize;
            msg = static_cast<MessageHdr*>(malloc(msgsize * sizeof(char)));

            msg->msgType = MsgTypes::MEMSHIP;
            memcpy(static_cast<void*>((char*)(msg+1)),
                   static_cast<void*>(this->memberNode->memberList.data()),
                   memListSize);

            emulNet->ENsend(&memberNode->addr, &entryAddr,
                            reinterpret_cast<char*>(msg), msgsize);
            free(msg);
        }
    }

    if (this->memberNode->pingCounter == 0) {
        // Re-initialize the ping counter.
        this->memberNode->pingCounter = TFAIL;
    }

    // Update the local heartbeat.
    this->memberNode->heartbeat++;
    this->memberNode->pingCounter--;

    return;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;    
}
