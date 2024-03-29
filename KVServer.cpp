#include "header.hpp"
#include "KVCache.cpp"
#include "KVStore.cpp"
#include "ThreadPool.h"
#include <vector>
#include <map>
#include <iostream>
#include <openssl/sha.h>
#include <netinet/in.h>

typedef long long int lli;
using namespace std;


class PORTClass {
private:
    int portNoServer;
    int sock;
    struct sockaddr_in current;

public:
    void setPortServer();

    string getIpAddress();

    int getPortNumber();

    int getSocketFd();
};

class ServerClass {

public:
    PORTClass sp{};

    ServerClass();

    pair<pair<string, int>, lli> findSuccessor(lli nodeId);

    pair<pair<string, int>, lli> closestPrecedingNode(lli nodeId);

    void fixFingers();

    void stabilize();

    void notify(pair<pair<string, int>, lli> node);


    void updateSuccessorList();

    void setSuccessor(string ip, int port, lli hash);

    void setSuccessorList(string ip, int port, lli hash);

    void setFingerTable(string ip, int port, lli hash);





    bool isInRing;
    pair<pair<string, int>, lli> predecessor;
    lli id{};
    vector<pair<pair<string, int>, lli> > fingerTable;
    pair<pair<string, int>, lli> successor;
    vector<pair<pair<string, int>, lli> > successorList;
};


class HelperFunctions {

public:

    string combineIpAndPort(string ip, string port);

    vector<pair<string, int> > seperateSuccessorList(string succList);

    string splitSuccessorList(vector<pair<pair<string, int>, lli> > list);

    lli getHash(string key);

    pair<string, int> getIpAndPort(string key);


    bool isNodeAlive(string ip, int port);

    void setServerDetails(struct sockaddr_in &server, string ip, int port);

    void setTimer(struct timeval &timer);


    pair<pair<string, int>, lli>
    getPredecessorNode(string ip, int port, string ipClient, int ipPort, bool forStabilize);

    lli getSuccessorId(string ip, int port);

    void sendPredecessor(ServerClass nodeInfo, int newSock, struct sockaddr_in client);

    void sendSuccessor(ServerClass nodeInfo, string nodeIdString, int newSock, struct sockaddr_in client);

    void sendSuccessorId(ServerClass nodeInfo, int newSock, struct sockaddr_in client);

    void sendAcknowledgement(int newSock, struct sockaddr_in client);

    vector<pair<string, int> > getSuccessorListFromNode(string ip, int port);

    void sendSuccessorList(ServerClass &nodeInfo, int sock, struct sockaddr_in client);
};


/* generate a port number to run on */
void PORTClass::setPortServer() {

    /* generating a port number between 1024 and 65535 */
    srand(time(nullptr));
    portNoServer = rand() % 65536;
    if (portNoServer < 1024)
        portNoServer += 1024;

    socklen_t len = sizeof(current);

    sock = socket(AF_INET, SOCK_DGRAM, 0);
    current.sin_family = AF_INET;
    current.sin_port = htons(portNoServer);
    current.sin_addr.s_addr = inet_addr("127.0.0.1");

    if (bind(sock, (struct sockaddr *) &current, len) < 0) {
        perror("error");
        exit(-1);
    }

}

/* get IP Address */
string PORTClass::getIpAddress() {
    string ip = inet_ntoa(current.sin_addr);
    return ip;
}

/* get port number on which it is listening */
int PORTClass::getPortNumber() {
    return portNoServer;
}

/* */
int PORTClass::getSocketFd() {
    return sock;
}


ServerClass::ServerClass() {
    fingerTable = vector<pair<pair<string, int>, lli> >(M + 1);
    successorList = vector<pair<pair<string, int>, lli> >(R + 1);
    isInRing = false;
}


void ServerClass::setSuccessor(string ip, int port, lli hash) {
    successor.first.first = ip;
    successor.first.second = port;
    successor.second = hash;
}

void ServerClass::setSuccessorList(string ip, int port, lli hash) {
    for (int i = 1; i <= R; i++) {
        successorList[i] = make_pair(make_pair(ip, port), hash);
    }
}

void ServerClass::setFingerTable(string ip, int port, lli hash) {
    for (int i = 1; i <= M; i++) {
        fingerTable[i] = make_pair(make_pair(ip, port), hash);
    }
}


void ServerClass::updateSuccessorList() {

    HelperFunctions help;

    vector<pair<string, int> > list = help.getSuccessorListFromNode(successor.first.first, successor.first.second);

    if (list.size() != R)
        return;

    successorList[1] = successor;

    for (int i = 2; i <= R; i++) {
        successorList[i].first.first = list[i - 2].first;
        successorList[i].first.second = list[i - 2].second;
        successorList[i].second = help.getHash(list[i - 2].first + ":" + to_string(list[i - 2].second));
    }

}


pair<pair<string, int>, lli> ServerClass::findSuccessor(lli nodeId) {

    pair<pair<string, int>, lli> self;
    self.first.first = sp.getIpAddress();
    self.first.second = sp.getPortNumber();
    self.second = id;

    if (nodeId > id && nodeId <= successor.second) {
        return successor;
    }

        /* */
    else if (id == successor.second || nodeId == id) {
        return self;
    } else if (successor.second == predecessor.second) {
        if (successor.second >= id) {
            if (nodeId > successor.second || nodeId < id)
                return self;
        } else {
            if ((nodeId > id && nodeId > successor.second) || (nodeId < id && nodeId < successor.second))
                return successor;
            else
                return self;
        }
    } else {

        pair<pair<string, int>, lli> node = closestPrecedingNode(nodeId);
        if (node.second == id) {
            return successor;
        } else {

            /* connect to node which will now find the successor */
            struct sockaddr_in serverToConnectTo;
            socklen_t len = sizeof(serverToConnectTo);

            string ip;
            int port;

            /* if this node couldn't find closest preciding node for given node id then now ask it's successor to do so */
            if (node.second == -1) {
                node = successor;
            }

            HelperFunctions help;

            help.setServerDetails(serverToConnectTo, node.first.first, node.first.second);

            /* set timer on this socket */
            struct timeval timer;
            help.setTimer(timer);


            int sockT = socket(AF_INET, SOCK_DGRAM, 0);

            setsockopt(sockT, SOL_SOCKET, SO_RCVTIMEO, (char *) &timer, sizeof(struct timeval));

            if (sockT < 0) {
                perror("error");
                exit(-1);
            }

            /* send the node's id to the other node */
            char nodeIdChar[40];
            strcpy(nodeIdChar, to_string(nodeId).c_str());
            sendto(sockT, nodeIdChar, strlen(nodeIdChar), 0, (struct sockaddr *) &serverToConnectTo, len);

            /* receive ip and port of node's successor as ip:port*/
            char ipAndPort[40];

            int l = recvfrom(sockT, ipAndPort, 1024, 0, (struct sockaddr *) &serverToConnectTo, &len);

            close(sockT);

            if (l < 0) {
                pair<pair<string, int>, lli> node;
                node.first.first = "";
                node.second = -1;
                node.first.second = -1;
                return node;
            }

            ipAndPort[l] = '\0';

            /* set ip,port and hash for this node and return it */
            string key = ipAndPort;
            lli hash = help.getHash(ipAndPort);
            pair<string, int> ipAndPortPair = help.getIpAndPort(key);
            node.first.first = ipAndPortPair.first;
            node.first.second = ipAndPortPair.second;
            node.second = hash;

            return node;
        }
    }
}

pair<pair<string, int>, lli> ServerClass::closestPrecedingNode(lli nodeId) {
    HelperFunctions help;

    for (int i = M; i >= 1; i--) {
        if (fingerTable[i].first.first == "" || fingerTable[i].first.second == -1 || fingerTable[i].second == -1) {
            continue;
        }

        if (fingerTable[i].second > id && fingerTable[i].second < nodeId) {
            return fingerTable[i];
        } else {

            lli successorId = help.getSuccessorId(fingerTable[i].first.first, fingerTable[i].first.second);

            if (successorId == -1)
                continue;

            if (fingerTable[i].second > successorId) {
                if ((nodeId <= fingerTable[i].second && nodeId <= successorId) ||
                    (nodeId >= fingerTable[i].second && nodeId >= successorId)) {
                    return fingerTable[i];
                }
            } else if (fingerTable[i].second < successorId && nodeId > fingerTable[i].second && nodeId < successorId) {
                return fingerTable[i];
            }

            pair<pair<string, int>, lli> predNode = help.getPredecessorNode(fingerTable[i].first.first,
                                                                            fingerTable[i].first.second, "", -1, false);
            lli predecessorId = predNode.second;

            if (predecessorId != -1 && fingerTable[i].second < predecessorId) {
                if ((nodeId <= fingerTable[i].second && nodeId <= predecessorId) ||
                    (nodeId >= fingerTable[i].second && nodeId >= predecessorId)) {
                    return predNode;
                }
            }
            if (predecessorId != -1 && fingerTable[i].second > predecessorId && nodeId >= predecessorId &&
                nodeId <= fingerTable[i].second) {
                return predNode;
            }
        }
    }

    /* */
    pair<pair<string, int>, lli> node;
    node.first.first = "";
    node.first.second = -1;
    node.second = -1;
    return node;
}

void ServerClass::stabilize() {

    /* get predecessor of successor */

    HelperFunctions help;

    string ownIp = sp.getIpAddress();
    int ownPort = sp.getPortNumber();

    if (help.isNodeAlive(successor.first.first, successor.first.second) == false)
        return;

    /* get predecessor of successor */
    pair<pair<string, int>, lli> predNode = help.getPredecessorNode(successor.first.first, successor.first.second,
                                                                    ownIp, ownPort, true);

    lli predecessorHash = predNode.second;

    if (predecessorHash == -1 || predecessor.second == -1)
        return;

    if (predecessorHash > id || (predecessorHash > id && predecessorHash < successor.second) ||
        (predecessorHash < id && predecessorHash < successor.second)) {
        successor = predNode;
    }


}




void ServerClass::notify(pair<pair<string, int>, lli> node) {

    predecessor = node;

    /* if node's successor is node itself then set it's successor to this node */
    if (successor.second == id) {
        successor = node;
    }
}

void ServerClass::fixFingers() {

    HelperFunctions help;


    int next = 1;
    lli mod = pow(2, M);

    while (next <= M) {
        if (!help.isNodeAlive(successor.first.first, successor.first.second))
            return;

        lli newId = id + pow(2, next - 1);
        newId = newId % mod;
        pair<pair<string, int>, lli> node = findSuccessor(newId);
        if (node.first.first == "" || node.second == -1 || node.first.second == -1)
            break;
        fingerTable[next] = node;
        next++;
    }

}


/* get SHA1 hash for a given key */
lli HelperFunctions::getHash(string key) {
    unsigned char obuf[41];
    char finalHash[41];
    string keyHash = "";
    int i;
    lli mod = pow(2, M);


    /* convert string to an unsigned char array because SHA1 takes unsigned char array as parameter */
    unsigned char unsigned_key[key.length() + 1];
    for (i = 0; i < key.length(); i++) {
        unsigned_key[i] = key[i];
    }
    unsigned_key[i] = '\0';


    SHA1(unsigned_key, sizeof(unsigned_key), obuf);
    for (i = 0; i < M / 8; i++) {
        sprintf(finalHash, "%d", obuf[i]);
        keyHash += finalHash;
    }

    lli hash = stoll(keyHash) % mod;

    return hash;
}

/* key will be in form of ip:port , will seperate ip and port and return it */
pair<string, int> HelperFunctions::getIpAndPort(string key) {

    int pos = key.find(':');
    string ip = key.substr(0, pos);
    string port = key.substr(pos + 1);

    pair<string, int> ipAndPortPair;
    ipAndPortPair.first = ip;
    ipAndPortPair.second = atoi(port.c_str());

    return ipAndPortPair;
}

/* set details of server to which you want to connect to */
void HelperFunctions::setServerDetails(struct sockaddr_in &server, string ip, int port) {
    server.sin_family = AF_INET;
    server.sin_addr.s_addr = inet_addr(ip.c_str());
    server.sin_port = htons(port);
}

/* string is in form of ip:port;ip:port;... will seperate all these ip's and ports */
vector<pair<string, int> > HelperFunctions::seperateSuccessorList(string succList) {
    int size = succList.size();
    int i = 0;
    vector<pair<string, int> > res;

    while (i < size) {
        string ip = "";
        while (i < size && succList[i] != ':') {
            ip += succList[i];
            i++;
        }
        i++;

        string port = "";
        while (i < size && succList[i] != ';') {
            port += succList[i];
            i++;
        }
        i++;

        res.push_back(make_pair(ip, stoi(port)));
    }

    return res;
}

/* combine ip and port as ip:port and return string */
string HelperFunctions::combineIpAndPort(string ip, string port) {
    string ipAndPort = "";
    int i = 0;

    for (i = 0; i < ip.size(); i++) {
        ipAndPort += ip[i];
    }

    ipAndPort += ':';

    for (i = 0; i < port.size(); i++) {
        ipAndPort += port[i];
    }

    return ipAndPort;
}


/* send successor id of current node to the contacting node */
void HelperFunctions::sendSuccessorId(ServerClass nodeInfo, int newSock, struct sockaddr_in client) {

    pair<pair<string, int>, lli> succ = nodeInfo.successor;
    string succId = to_string(succ.second);
    char succIdChar[40];

    socklen_t l = sizeof(client);

    strcpy(succIdChar, succId.c_str());

    sendto(newSock, succIdChar, strlen(succIdChar), 0, (struct sockaddr *) &client, l);

}

/* find successor of contacting node and send it's ip:port to it */
void HelperFunctions::sendSuccessor(ServerClass nodeInfo, string nodeIdString, int newSock, struct sockaddr_in client) {

    lli nodeId = stoll(nodeIdString);

    socklen_t l = sizeof(client);

    /* find successor of the joining node */
    pair<pair<string, int>, lli> succNode;
    succNode = nodeInfo.findSuccessor(nodeId);

    /* get Ip and port of successor as ip:port in char array to send */
    char ipAndPort[40];
    string succIp = succNode.first.first;
    string succPort = to_string(succNode.first.second);
    strcpy(ipAndPort, combineIpAndPort(succIp, succPort).c_str());

    /* send ip and port info to the respective node */
    sendto(newSock, ipAndPort, strlen(ipAndPort), 0, (struct sockaddr *) &client, l);

}

/* send ip:port of predecessor of current node to contacting node */
void HelperFunctions::sendPredecessor(ServerClass nodeInfo, int newSock, struct sockaddr_in client) {

    pair<pair<string, int>, lli> predecessor = nodeInfo.predecessor;

    string ip = predecessor.first.first;
    string port = to_string(predecessor.first.second);

    socklen_t l = sizeof(client);

    /* if predecessor is nil */
    if (ip == "") {
        sendto(newSock, "", 0, 0, (struct sockaddr *) &client, l);
    } else {
        string ipAndPort = combineIpAndPort(ip, port);

        char ipAndPortChar[40];
        strcpy(ipAndPortChar, ipAndPort.c_str());

        sendto(newSock, ipAndPortChar, strlen(ipAndPortChar), 0, (struct sockaddr *) &client, l);

    }
}

/* get successor id of the node having ip address as ip and port num as port */
lli HelperFunctions::getSuccessorId(string ip, int port) {

    struct sockaddr_in serverToConnectTo;
    socklen_t l = sizeof(serverToConnectTo);

    setServerDetails(serverToConnectTo, ip, port);

    /* set timer for socket */
    struct timeval timer;
    setTimer(timer);

    int sock = socket(AF_INET, SOCK_DGRAM, 0);

    if (sock < 0) {
        perror("error");
        exit(-1);
    }

    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, (char *) &timer, sizeof(struct timeval));

    if (sock < -1) {
        cout << "socket cre error";
        perror("error");
        exit(-1);
    }

    char msg[] = "finger";

    if (sendto(sock, msg, strlen(msg), 0, (struct sockaddr *) &serverToConnectTo, l) == -1) {
        perror("error");
        exit(-1);
    }

    char succIdChar[40];

    int len = recvfrom(sock, succIdChar, 1024, 0, (struct sockaddr *) &serverToConnectTo, &l);

    close(sock);

    if (len < 0) {
        return -1;
    }

    succIdChar[len] = '\0';

    return atoll(succIdChar);

}

void HelperFunctions::setTimer(struct timeval &timer) {
    timer.tv_sec = 0;
    timer.tv_usec = 100000;
}

/* get predecessor node (ip:port) of the node having ip and port */
pair<pair<string, int>, lli>
HelperFunctions::getPredecessorNode(string ip, int port, string ipClient, int portClient, bool forStabilize) {

    struct sockaddr_in serverToConnectTo;
    socklen_t l = sizeof(serverToConnectTo);

    setServerDetails(serverToConnectTo, ip, port);

    /* set timer for socket */
    struct timeval timer;
    setTimer(timer);

    int sock = socket(AF_INET, SOCK_DGRAM, 0);

    if (sock < 0) {
        perror("error");
        exit(-1);
    }

    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, (char *) &timer, sizeof(struct timeval));

    string msg = "";

    /* p2 means that just send predecessor of node ip:port , do not call notify */
    /* p1 means that this is for stabilize so notify node as well */

    if (forStabilize == true) {
        msg = combineIpAndPort(ipClient, to_string(portClient));
        msg += "p1";
    } else
        msg = "p2";


    char ipAndPortChar[40];
    strcpy(ipAndPortChar, msg.c_str());

    if (sendto(sock, ipAndPortChar, strlen(ipAndPortChar), 0, (struct sockaddr *) &serverToConnectTo, l) < 0) {
        perror("error");
        exit(-1);
    }


    int len = recvfrom(sock, ipAndPortChar, 1024, 0, (struct sockaddr *) &serverToConnectTo, &l);
    close(sock);

    if (len < 0) {
        pair<pair<string, int>, lli> node;
        node.first.first = "";
        node.first.second = -1;
        node.second = -1;
        return node;
    }

    ipAndPortChar[len] = '\0';


    string ipAndPort = ipAndPortChar;
    lli hash;
    pair<string, int> ipAndPortPair;

    pair<pair<string, int>, lli> node;

    if (ipAndPort == "") {
        node.first.first = "";
        node.first.second = -1;
        node.second = -1;
    } else {
        ipAndPortPair = getIpAndPort(ipAndPort);
        node.first.first = ipAndPortPair.first;
        node.first.second = ipAndPortPair.second;
        node.second = getHash(ipAndPort);
    }

    return node;
}

/* get successor list from node having ip and port */
vector<pair<string, int> > HelperFunctions::getSuccessorListFromNode(string ip, int port) {

    struct sockaddr_in serverToConnectTo;
    socklen_t l = sizeof(serverToConnectTo);

    setServerDetails(serverToConnectTo, ip, port);

    /* set timer for socket */
    struct timeval timer;
    setTimer(timer);


    int sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock < 0) {
        perror("error");
        exit(-1);
    }

    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, (char *) &timer, sizeof(struct timeval));

    char msg[] = "sendSuccList";

    sendto(sock, msg, strlen(msg), 0, (struct sockaddr *) &serverToConnectTo, l);

    char succListChar[1000];
    int len = recvfrom(sock, succListChar, 1000, 0, (struct sockaddr *) &serverToConnectTo, &l);

    close(sock);


    if (len < 0) {
        vector<pair<string, int> > list;
        return list;
    }

    succListChar[len] = '\0';

    string succList = succListChar;

    vector<pair<string, int> > list = seperateSuccessorList(succList);

    return list;

}

/* send node's successor list to the contacting node */
void HelperFunctions::sendSuccessorList(ServerClass &nodeInfo, int sock, struct sockaddr_in client) {
    socklen_t l = sizeof(client);

    vector<pair<pair<string, int>, lli> > list = nodeInfo.successorList;

    string successorList = splitSuccessorList(list);

    char successorListChar[1000];
    strcpy(successorListChar, successorList.c_str());

    sendto(sock, successorListChar, strlen(successorListChar), 0, (struct sockaddr *) &client, l);

}

/* combine successor list in form of ip1:port1;ip2:port2;.. */
string HelperFunctions::splitSuccessorList(vector<pair<pair<string, int>, lli> > list) {
    string res = "";

    for (int i = 1; i <= R; i++) {

        res = res + list[i].first.first + ":" + to_string(list[i].first.second) + ";";
    }

    return res;
}

/* send ack to contacting node that this node is still alive */
void HelperFunctions::sendAcknowledgement(int newSock, struct sockaddr_in client) {
    socklen_t l = sizeof(client);

    sendto(newSock, "1", 1, 0, (struct sockaddr *) &client, l);
}

/* check if node having ip and port is still alive or not */
bool HelperFunctions::isNodeAlive(string ip, int port) {
    struct sockaddr_in serverToConnectTo;
    socklen_t l = sizeof(serverToConnectTo);

    setServerDetails(serverToConnectTo, ip, port);

    /* set timer for socket */
    struct timeval timer;
    setTimer(timer);


    int sock = socket(AF_INET, SOCK_DGRAM, 0);

    if (sock < 0) {
        perror("error");
        exit(-1);
    }

    /* set timer on this socket */
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, (char *) &timer, sizeof(struct timeval));

    char msg[] = "alive";
    sendto(sock, msg, strlen(msg), 0, (struct sockaddr *) &serverToConnectTo, l);

    char response[5];
    int len = recvfrom(sock, response, 2, 0, (struct sockaddr *) &serverToConnectTo, &l);

    close(sock);

    /* node is still active */
    if (len >= 0) {
        return true;
    } else
        return false;
}

typedef long long int lli;

void listenTo(ServerClass &nodeInfo);

void doStabilize(ServerClass &nodeInfo);

void callNotify(ServerClass &nodeInfo, string ipAndPort);

void doTask(ServerClass &nodeInfo, int newSock, struct sockaddr_in client, string nodeIdString);


typedef long long int lli;

using namespace std;

HelperFunctions help = HelperFunctions();



/* perform different tasks according to received msg */
void doTask(ServerClass &nodeInfo, int newSock, struct sockaddr_in client, string nodeIdString) {


    if (nodeIdString.find("alive") != -1) {
        help.sendAcknowledgement(newSock, client);
    }

        /* contacting node wants successor list of this node */
    else if (nodeIdString.find("sendSuccList") != -1) {
        help.sendSuccessorList(nodeInfo, newSock, client);
    }

        /* contacting node wants the predecessor of this node */
    else if (nodeIdString.find("p") != -1) {
        help.sendPredecessor(nodeInfo, newSock, client);

        /* p1 in msg means that notify the current node about this contacting node */
        if (nodeIdString.find("p1") != -1) {
            callNotify(nodeInfo, nodeIdString);
        }
    }

        /* contacting node wants successor Id of this node for help in finger table */
    else if (nodeIdString.find("finger") != -1) {
        help.sendSuccessorId(nodeInfo, newSock, client);
    }

        /* contacting node wants current node to find successor for it */
    else {
        help.sendSuccessor(nodeInfo, nodeIdString, newSock, client);
    }

}

/* listen to any contacting node */
void listenTo(ServerClass &nodeInfo) {
    struct sockaddr_in client;
    socklen_t l = sizeof(client);

    /* wait for any client to connect and create a new thread as soon as one connects */
    while (1) {
        char charNodeId[40];
        int sock = nodeInfo.sp.getSocketFd();
        int len = recvfrom(sock, charNodeId, 1024, 0, (struct sockaddr *) &client, &l);
        charNodeId[len] = '\0';
        string nodeIdString = charNodeId;

        /* launch a thread that will perform diff tasks acc to received msg */
        thread f(doTask, ref(nodeInfo), sock, client, nodeIdString);
        f.detach();
    }
}


void doStabilize(ServerClass &nodeInfo) {

    /* do stabilize tasks */
    while (1) {



        nodeInfo.stabilize();

        nodeInfo.updateSuccessorList();

        nodeInfo.fixFingers();

        this_thread::sleep_for(chrono::milliseconds(300));
    }
}

/* call notify of current node which will notify curr node of contacting node */
void callNotify(ServerClass &nodeInfo, string ipAndPort) {

    ipAndPort.pop_back();
    ipAndPort.pop_back();

    /* get ip and port of client node */
    pair<string, int> ipAndPortPair = help.getIpAndPort(ipAndPort);
    string ip = ipAndPortPair.first;
    int port = ipAndPortPair.second;
    lli hash = help.getHash(ipAndPort);

    pair<pair<string, int>, lli> node;
    node.first.first = ip;
    node.first.second = port;
    node.second = hash;

    /* notify current node about this node */
    nodeInfo.notify(node);
}

int PORT;

ServerClass nodeInfo = ServerClass();

KVCache cacheMap;

class Node {
    std::string IPAddress;
    int finger[number_of_bits]{1};

public:


    Node() {
        IPAddress = IP;
        run();

    }


    virtual ~Node() = default;


    static void HandleRequest(int new_socket, int valread, const char *buffer1) {

        std::thread::id this_id = std::this_thread::get_id();

//        g_display_mutex.lock();

        if (debugger_mode) {
            cout << buffer1 << "\n";

        }

        std::string buffer;
        for (int i = 0; i < valread; i++) {
            buffer += (buffer1[i]);
        }


        std::string buffer2 = fromxml(buffer);

        char chararr_of_buffer[buffer2.length() + 1];
        strcpy(chararr_of_buffer, buffer2.c_str());

        // Extract request type
        std::string request_type = strtok(chararr_of_buffer, delimiter);
        if (debugger_mode) {
            std::cout << request_type << '\n';
        }
        // Extract key
        std::string key = strtok(nullptr, delimiter);
        if (debugger_mode) {
            std::cout << key << '\n';
        }

        std::string value;
        std::string response;
        std::string error_msg = "Error Message";
        char return_value[max_buffer_size];
        // Extract value if the request type is PUT



        if (request_type == "PUT") {
            value = strtok(nullptr, delimiter);
            if (debugger_mode) {
                cout << "Value=" << value << "\n";
            }

            response = "Success";
        } else if (request_type == "DEL") {
            value = "";
            if (debugger_mode) {
                cout << "Value=" << value << "\n";
            }



            response = "Success";
        } else if (request_type == "GET") {
                cout << "Sorry this node is not in the ring\n";
                response = "Does not exist";
                response = "Finding";
        } else {
            response = error_msg;
        }
        response = toXML(response);
        strcpy(return_value, response.c_str());

        if (debugger_mode) {
            cout << "Response: \n" << response;
        }
//    cout << return_value << std::endl;
        send(new_socket, return_value, sizeof(return_value), 0);
//        close(new_socket);
    }


    static int run() {

        int server_fd = socket(AF_INET, SOCK_STREAM, 0);
        struct sockaddr_in address = {address.sin_family = AF_INET,
                address.sin_port = htons(PORT),
                address.sin_addr.s_addr = INADDR_ANY};
        int new_socket, valread;
        char buffer1[max_buffer_size] = {0};
        int opt = 1;
        int addr_len = sizeof(address);

        setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt));

        //binding the address to the socket..address is the ip adress of the machine
        bind(server_fd, (struct sockaddr *) &address, sizeof(address));

        /*************************************************************/
        /* Set the listen back log                                   */
        /*************************************************************/
        int rc = listen(server_fd, numOfTotalFDs);
        if (rc < 0) {
            perror("Network Error: listen() failed");
            close(server_fd);
            exit(-1);
        }


        ThreadPool pool(threadPoolSize);

// Initialize pool
        pool.init();


        int i = 0;


        // Server runs forever
        while (True) {
            //accept creates a new socket for comunication
            new_socket = accept(server_fd, (struct sockaddr *) &address, (socklen_t *) &(addr_len));
            if (debugger_mode) {
                cout << ++i;
                cout << "connection made with client fd==========>" << new_socket << "\n";
            }
            //reading from the socket
            valread = read(new_socket, buffer1, max_buffer_size);
            buffer1[valread] = '\0';

            auto future = pool.submit(HandleRequest, new_socket, valread, buffer1);
            future.get();


        }
    }

};


int main(int argc, char *argv[]) {


    cout << "============================================================================\n"
            "|  To dump the KVStore key value pairs to a file, use command:              |\n"
            "|  ./KVServer dumpToFile [filename]                                         |\n"
            "======================================OR====================================\n"
            "|  To restore the key value pairs from a file to the KVStore, use command:  |\n"
            "|  ./KVServer restoreFromFile [filename]                                    |\n"
            "============================================================================\n";

    KVStore kvStore;
    if (argc == 3) {
        if (strcmp(argv[1], "restoreFromFile") == 0) {
            kvStore.RestoreFromFile(argv[2]);
            cout << "Restore from file " << argv[2] << " successful." << std::endl;
        } else if (strcmp(argv[1], "dumpToFile") == 0) {
            kvStore.dumpToFile(argv[2]);
            cout << "Dump to file " << argv[2] << " successful." << std::endl;
        }

    }

    nodeInfo.sp.setPortServer();
    PORT = nodeInfo.sp.getPortNumber();
    cout << "Now listening at port number " << nodeInfo.sp.getPortNumber() << endl;


    int choice;
    while (true) {
        cout << "======================================\n"
                "Enter 1 to create a new ring\n"
                "Enter 2 to join some existing ring\n"
                "Enter 3 to display finger table\n"
                "======================================\n";

        cin >> choice;
        if (choice == 1) {


                string ip = nodeInfo.sp.getIpAddress();
                int port = nodeInfo.sp.getPortNumber();

                /* key to be hashed for a node is ip:port */
                string key = ip + ":" + (to_string(port));

                lli hash = help.getHash(key);

                nodeInfo.id = hash;

                nodeInfo.setSuccessor(ip, port, hash);
                nodeInfo.setSuccessorList(ip, port, hash);
                nodeInfo.predecessor.first.first = ""; //IP
                nodeInfo.predecessor.first.second = -1; //PORT
                nodeInfo.predecessor.second = -1; //HASH
                nodeInfo.setFingerTable(ip, port, hash);
                nodeInfo.isInRing = true;;

                thread second(listenTo, ref(nodeInfo));
                second.detach();

                thread fifth(doStabilize, ref(nodeInfo));
                fifth.detach();
        } else if (choice == 2) {
            string ip = "127.0.0.1";
            string port;
            cout << "Enter port number \n";
//            cin>>ip;
            cin >> port;

                struct sockaddr_in server;

                socklen_t l = sizeof(server);

                help.setServerDetails(server, ip, stoi(port));

                int sock = socket(AF_INET, SOCK_DGRAM, 0);

                if (sock < 0) {
                    perror("error");
                    exit(-1);
                }

                string currIp = nodeInfo.sp.getIpAddress();
                string currPort = to_string(nodeInfo.sp.getPortNumber());

                /* generate id of current node */
                lli nodeId = help.getHash(currIp + ":" + currPort);

                char charNodeId[41];
                strcpy(charNodeId, to_string(nodeId).c_str());


                /* node sends it's id to main node to find it's successor */
                if (sendto(sock, charNodeId, strlen(charNodeId), 0, (struct sockaddr *) &server, l) == -1) {
                    perror("error");
                    exit(-1);
                }

                /* node receives id and port of it's successor */
                char ipAndPort[40];
                int len;
                if ((len = recvfrom(sock, ipAndPort, 1024, 0, (struct sockaddr *) &server, &l)) == -1) {
                    perror("error");
                    exit(-1);
                }
                ipAndPort[len] = '\0';

                close(sock);

                cout << "Successfully joined the ring\n";

                string key = ipAndPort;
                lli hash = help.getHash(key);
                pair<string, int> ipAndPortPair = help.getIpAndPort(key);

                /* setting id, successor , successor list , predecessor, finger table and status */
                nodeInfo.id = nodeId;

                nodeInfo.setSuccessor(ipAndPortPair.first, ipAndPortPair.second, hash);
                nodeInfo.setSuccessorList(ipAndPortPair.first, ipAndPortPair.second, hash);
                nodeInfo.predecessor.first.first = ""; //IP
                nodeInfo.predecessor.first.second = -1; //PORT
                nodeInfo.predecessor.second = -1; //HASH
                nodeInfo.setFingerTable(ipAndPortPair.first, ipAndPortPair.second, hash);
                nodeInfo.isInRing = true;


                thread listenerThread(listenTo, ref(nodeInfo));
                listenerThread.detach();

                thread StabalizerThread(doStabilize, ref(nodeInfo));
                StabalizerThread.detach();


        } else if (choice == 3) {
            int port;
            vector<pair<pair<string, int>, lli> > succList = nodeInfo.successorList;
            lli id = nodeInfo.id;
            cout<<"Curr ID: "<<id<<std::endl;
            for (int i = 1; i <= M; i++) {
                port = nodeInfo.fingerTable[i].first.second;
                lli hash = nodeInfo.fingerTable[i].second;
                cout << i << ": " << port << " "<< hash <<endl;
            }
        } else {
            cout << "Wrong choice enter again\n";
        }
    }
    Node node;
    Node::run();
}