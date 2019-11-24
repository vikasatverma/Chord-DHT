#include "headers.h"
#include <openssl/sha.h>
#include "helperClass.h"

mutex mt;

/* get SHA1 hash for a given key */
lli HelperFunctions::getHash(string key){
    unsigned char obuf[41];
    char finalHash[41];
    string keyHash = "";
    int i;
    lli mod = pow(2,M);

    
    /* convert string to an unsigned char array because SHA1 takes unsigned char array as parameter */
    unsigned char unsigned_key[key.length()+1];
    for(i=0;i<key.length();i++){
        unsigned_key[i] = key[i];
    }
    unsigned_key[i] = '\0';


    SHA1(unsigned_key,sizeof(unsigned_key),obuf);
    for (i = 0; i < M/8; i++) {
        sprintf(finalHash,"%d",obuf[i]);
        keyHash += finalHash;
    }

    lli hash = stoll(keyHash) % mod;

    return hash;
}

/* key will be in form of ip:port , will seperate ip and port and return it */
pair<string,int> HelperFunctions::getIpAndPort(string key){

    int pos = key.find(':');
    string ip = key.substr(0,pos);
    string port = key.substr(pos+1);

    pair<string,int> ipAndPortPair;
    ipAndPortPair.first = ip;
    ipAndPortPair.second = atoi(port.c_str());

    return ipAndPortPair;
}
/* set details of server to which you want to connect to */
void HelperFunctions::setServerDetails(struct sockaddr_in &server,string ip,int port){
    server.sin_family = AF_INET;
    server.sin_addr.s_addr = inet_addr(ip.c_str());
    server.sin_port = htons(port);
}
/* string is in form of ip:port;ip:port;... will seperate all these ip's and ports */
vector< pair<string,int> > HelperFunctions::seperateSuccessorList(string succList){
    int size = succList.size();
    int i = 0;
    vector< pair<string,int> > res;

    while(i < size){
        string ip = "";
        while(i < size && succList[i] != ':'){
            ip += succList[i];
            i++;
        }
        i++;

        string port = "";
        while(i < size && succList[i] != ';'){
            port += succList[i];
            i++;
        }
        i++;

        res.push_back(make_pair(ip,stoi(port)));
    }

    return res;
}

/* combine ip and port as ip:port and return string */
string HelperFunctions::combineIpAndPort(string ip,string port){
    string ipAndPort = "";
    int i=0;

    for(i=0;i<ip.size();i++){
        ipAndPort += ip[i];
    }

    ipAndPort += ':';

    for(i=0;i<port.size();i++){
        ipAndPort += port[i];
    }

    return ipAndPort;
}

/* */
void HelperFunctions::sendValToNode(NodeInformation nodeInfo,int newSock,struct sockaddr_in client,string nodeIdString){
    nodeIdString.pop_back();
    lli key = stoll(nodeIdString);
    string val = nodeInfo.getValue(key);

    socklen_t l = sizeof(client);

    char valChar[100];
    strcpy(valChar,val.c_str());

    sendto(newSock,valChar,strlen(valChar),0,(struct sockaddr *)&client,l);
}

/* send successor id of current node to the contacting node */
void HelperFunctions::sendSuccessorId(NodeInformation nodeInfo,int newSock,struct sockaddr_in client){

    pair< pair<string,int> , lli > succ = nodeInfo.getSuccessor();
    string succId = to_string(succ.second);
    char succIdChar[40];

    socklen_t l = sizeof(client);

    strcpy(succIdChar,succId.c_str());

    sendto(newSock,succIdChar,strlen(succIdChar),0,(struct sockaddr *)&client,l);

}

/* find successor of contacting node and send it's ip:port to it */
void HelperFunctions::sendSuccessor(NodeInformation nodeInfo,string nodeIdString,int newSock,struct sockaddr_in client){
    
    lli nodeId = stoll(nodeIdString);

    socklen_t l = sizeof(client);
    
    /* find successor of the joining node */
    pair< pair<string,int> , lli > succNode;
    succNode = nodeInfo.findSuccessor(nodeId);

    /* get Ip and port of successor as ip:port in char array to send */
    char ipAndPort[40];
    string succIp = succNode.first.first;
    string succPort = to_string(succNode.first.second);
    strcpy(ipAndPort,combineIpAndPort(succIp,succPort).c_str());

    /* send ip and port info to the respective node */
    sendto(newSock, ipAndPort, strlen(ipAndPort), 0, (struct sockaddr*) &client, l);

}

/* send ip:port of predecessor of current node to contacting node */
void HelperFunctions::sendPredecessor(NodeInformation nodeInfo,int newSock,struct sockaddr_in client){
    
    pair< pair<string,int> , lli > predecessor = nodeInfo.getPredecessor();
    
    string ip = predecessor.first.first;
    string port = to_string(predecessor.first.second);

    socklen_t l = sizeof(client);

    /* if predecessor is nil */
    if(ip == ""){
        sendto(newSock, "", 0, 0, (struct sockaddr*) &client, l);       
    }

    else{
        string ipAndPort = combineIpAndPort(ip,port);

        char ipAndPortChar[40];
        strcpy(ipAndPortChar,ipAndPort.c_str());

        sendto(newSock, ipAndPortChar, strlen(ipAndPortChar), 0, (struct sockaddr*) &client, l);

    }
}

/* get successor id of the node having ip address as ip and port num as port */
lli HelperFunctions::getSuccessorId(string ip,int port){
    
    struct sockaddr_in serverToConnectTo;
    socklen_t l = sizeof(serverToConnectTo);

    setServerDetails(serverToConnectTo,ip,port);

    /* set timer for socket */
    struct timeval timer;
    setTimer(timer);

    int sock = socket(AF_INET,SOCK_DGRAM,0);

    if(sock < 0){
        perror("error");
        exit(-1);
    }

    setsockopt(sock,SOL_SOCKET,SO_RCVTIMEO,(char*)&timer,sizeof(struct timeval));

    if(sock < -1){
        cout<<"socket cre error";
        perror("error");
        exit(-1);
    }

    char msg[] = "finger";

    if (sendto(sock, msg, strlen(msg) , 0, (struct sockaddr*) &serverToConnectTo, l) == -1){
        perror("error");
        exit(-1);
    }

    char succIdChar[40];

    int len = recvfrom(sock,succIdChar,1024,0,(struct sockaddr*) &serverToConnectTo, &l);

    close(sock);

    if(len < 0){
        return -1;
    }

    succIdChar[len] = '\0';

    return atoll(succIdChar);

}

void HelperFunctions::setTimer(struct timeval &timer){
    timer.tv_sec = 0;
    timer.tv_usec = 100000;
}

/* get predecessor node (ip:port) of the node having ip and port */
pair< pair<string,int> , lli > HelperFunctions::getPredecessorNode(string ip,int port,string ipClient,int portClient,bool forStabilize){

    struct sockaddr_in serverToConnectTo;
    socklen_t l = sizeof(serverToConnectTo);

    setServerDetails(serverToConnectTo,ip,port);

    /* set timer for socket */
    struct timeval timer;
    setTimer(timer);

    int sock = socket(AF_INET,SOCK_DGRAM,0);

    if(sock < 0){
        perror("error");
        exit(-1);
    }

    setsockopt(sock,SOL_SOCKET,SO_RCVTIMEO,(char*)&timer,sizeof(struct timeval));

    string msg = "";

    /* p2 means that just send predecessor of node ip:port , do not call notify */
    /* p1 means that this is for stabilize so notify node as well */

    if(forStabilize == true){
        msg = combineIpAndPort(ipClient,to_string(portClient));
        msg += "p1";
    }

    else
        msg = "p2";


    char ipAndPortChar[40];
    strcpy(ipAndPortChar,msg.c_str());

    if (sendto(sock, ipAndPortChar, strlen(ipAndPortChar), 0, (struct sockaddr*) &serverToConnectTo, l) < 0){
        perror("error");
        exit(-1);
    }


    int len = recvfrom(sock, ipAndPortChar, 1024, 0, (struct sockaddr *) &serverToConnectTo, &l);
    close(sock);

    if(len < 0){
        pair< pair<string,int> , lli > node;
        node.first.first = "";
        node.first.second = -1;
        node.second = -1;
        return node;
    }

    ipAndPortChar[len] = '\0';

    

    string ipAndPort = ipAndPortChar;
    lli hash;
    pair<string,int> ipAndPortPair;

    pair< pair<string,int> , lli > node;

    if(ipAndPort == ""){
        node.first.first = "";
        node.first.second = -1;
        node.second = -1;
    }

    else{
        ipAndPortPair = getIpAndPort(ipAndPort);
        node.first.first = ipAndPortPair.first;
        node.first.second = ipAndPortPair.second;
        node.second = getHash(ipAndPort);
    }

    return node;
}

/* get successor list from node having ip and port */
vector< pair<string,int> > HelperFunctions::getSuccessorListFromNode(string ip,int port){

    struct sockaddr_in serverToConnectTo;
    socklen_t l = sizeof(serverToConnectTo);

    setServerDetails(serverToConnectTo,ip,port);

    /* set timer for socket */
    struct timeval timer;
    setTimer(timer);
    

    int sock = socket(AF_INET,SOCK_DGRAM,0);
    if(sock < 0){
        perror("error");
        exit(-1);
    }

    setsockopt(sock,SOL_SOCKET,SO_RCVTIMEO,(char*)&timer,sizeof(struct timeval));

    char msg[] = "sendSuccList";

    sendto(sock,msg,strlen(msg),0,(struct sockaddr *)&serverToConnectTo,l);

    char succListChar[1000];
    int len = recvfrom(sock,succListChar,1000,0,(struct sockaddr *)&serverToConnectTo,&l);

    close(sock);


    if(len < 0){
        vector< pair<string,int> > list;
        return list;
    }

    succListChar[len] = '\0';

    string succList = succListChar;

    vector< pair<string,int> > list = seperateSuccessorList(succList);

    return list;

}

/* send node's successor list to the contacting node */
void HelperFunctions::sendSuccessorList(NodeInformation &nodeInfo,int sock,struct sockaddr_in client){
    socklen_t l = sizeof(client);

    vector< pair< pair<string,int> , lli > > list = nodeInfo.getSuccessorList();

    string successorList = splitSuccessorList(list);

    char successorListChar[1000];
    strcpy(successorListChar,successorList.c_str());

    sendto(sock,successorListChar,strlen(successorListChar),0,(struct sockaddr *)&client,l);

}

/* combine successor list in form of ip1:port1;ip2:port2;.. */
string HelperFunctions::splitSuccessorList(vector< pair< pair<string,int> , lli > > list){
    string res = "";

    for(int i=1;i<=R;i++){

        res = res + list[i].first.first + ":" + to_string(list[i].first.second) + ";";
    }

    return res;
}

/* send ack to contacting node that this node is still alive */
void HelperFunctions::sendAcknowledgement(int newSock,struct sockaddr_in client){
    socklen_t l = sizeof(client);

    sendto(newSock,"1",1,0,(struct sockaddr*)&client,l);
}

/* check if node having ip and port is still alive or not */
bool HelperFunctions::isNodeAlive(string ip,int port){
    struct sockaddr_in serverToConnectTo;
    socklen_t l = sizeof(serverToConnectTo);

    setServerDetails(serverToConnectTo,ip,port);

    /* set timer for socket */
    struct timeval timer;
    setTimer(timer);


    int sock = socket(AF_INET,SOCK_DGRAM,0);

    if(sock < 0){
        perror("error");
        exit(-1);
    }

    /* set timer on this socket */
    setsockopt(sock,SOL_SOCKET,SO_RCVTIMEO,(char*)&timer,sizeof(struct timeval));

    char msg[] = "alive";
    sendto(sock,msg,strlen(msg),0,(struct sockaddr *)&serverToConnectTo,l);

    char response[5];
    int len = recvfrom(sock,response,2,0,(struct sockaddr *)&serverToConnectTo,&l);

    close(sock);

    /* node is still active */
    if(len >= 0){
        return true;
    }
    else
        return false;
}
