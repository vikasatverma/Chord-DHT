#include <iostream>

#include "headers.h"
#include "port.h"



/* generate a port number to run on */
void SocketAndPort::specifyPortServer(){

	/* generating a port number between 1024 and 65535 */
	srand(time(0));
	portNoServer = rand() % 65536;
	if(portNoServer < 1024)
		portNoServer += 1024;

	socklen_t len = sizeof(current);

	sock = socket(AF_INET,SOCK_DGRAM,0);
	current.sin_family = AF_INET;
	current.sin_port = htons(portNoServer);
	current.sin_addr.s_addr = inet_addr("127.0.0.1");

	if( bind(sock,(struct sockaddr *)&current,len) < 0){
		perror("error");
		exit(-1);
	}

}

/* get IP Address */
string SocketAndPort::getIpAddress(){
	string ip = inet_ntoa(current.sin_addr);
	return ip;
}

/* get port number on which it is listening */
int SocketAndPort::getPortNumber(){
	return portNoServer;
}

/* */
int SocketAndPort::getSocketFd(){
	return sock;
}
