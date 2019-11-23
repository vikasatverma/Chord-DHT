
#include <cmath>

#define debugger_mode 0

#include <cstring>
#include <bits/stdc++.h>
#include <cstdio>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <iostream>
#include <string>
#define max_key_lenght 256 // 256 Bytes
#define max_value_lenght (256*1024) // 256 KB
#define max_buffer_size (256 + 256*1024 + 4*2) // key_length + value_length + delimiter_length

using std::cout;
using std::cin;
int PORT;

std::vector<std::string> split(const char *str, char c = ' ') {
    std::vector<std::string> result;

    do {
        const char *begin = str;

        while (*str != c && *str)
            str++;

        result.emplace_back(begin, str);
    } while (0 != *str++);

    return result;
}



std::string toxml(std::string msg_type, std::string key, std::string value = "") {

    std::string request = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n";

    if (msg_type == "GET") {
        msg_type = "<KVMessage type=\"getreq\">\n";
        key = "<Key>" + key + "</Key>\n";
    }
    if (msg_type == "PUT") {

        msg_type = "<KVMessage type=\"putreq\">\n";
        key = "<key>" + key + "</Key>\n";
        value = "<Value>" + value + "</Value>\n";
        key = key + value;

    }
    if (msg_type == "DEL") {

        msg_type = "<KVMessage type=\"delreq\">\n";
        key = "<Key>" + key + "</Key>\n";
    }

    request = request + msg_type + key + "</KVMessage>\n";

    return request;
}

//to get back xml format to plain text
std::string xmltoplain(std::string str) {

    int i = 0;
    std::string plain_text;
    if (str[64] == 'M') {
        for (i = 72; str[i] != '<'; i++)
            plain_text += str[i];
    } else {
        for (i = 68; str[i] != '<'; i++)
            plain_text += str[i];
        plain_text += " ";
        int j = i + 14;
        for (; str[j] != '<'; j++)
            plain_text += str[j];

    }
    return plain_text;
}

int checkLenght(const std::string &key, const std::string &value = " ") {
    if (key.size() > max_key_lenght) {
        cout << "Oversized key";
        exit(-1);
    }
    if (value.size() > max_value_lenght) {
        cout << "Oversized value";
        exit(-1);
    }

}

//TODO: Increase capacity of request string to store 256KB

int main(int argc, char **argv) {

    int interactiveMode = 1;
//    cout<<argv[1];

    if (strcmp(argv[1], "port") == 0) {
        std::istringstream iss(argv[2]);
        int val;
        if (iss >> val) {
            // Conversion successful
        }
        PORT = val;
        cout << "Will run on port " << argv[2] << "." << std::endl;
    }
    if (argc == 5) {
        interactiveMode = 0;

    }
    std::ifstream infile(argv[3]);
    if (interactiveMode) {
        cout << "Enter request in the format\n"
                "GET,<key>\n"
                "PUT,<key>,<value>\n"
                "DEL,<key>\n"
                "=============================================OR=============================================\n"
                "Exit the interactive mode and provide two filenames in the commandline in the format\n"
                "./KVClient [inputfile] [outputfile]\n";
    }

    std::string request_type;
    std::string key;
    std::string value;
    char buffer1[max_buffer_size] = {0};
    int valread;
    std::string finalRequest;

    for (std::string line; interactiveMode || getline(infile, line);) {
        if (interactiveMode) {
            while (line.empty())
                getline(cin, line);
        }


        finalRequest = "";
        std::vector<std::string> request = split(line.c_str(), ',');
        if (debugger_mode) {
            cout << request[0] << "\t" << request[1] << "\t";
        }
        if (request.size() < 2 || request[0].empty() || request[1].empty()) {
            perror("XML Error: Received unparseable message");
            line.erase();
            continue;

        }
        request_type = request[0];
        key = request[1];
//        finalRequest.append(request[0]).append(delimiter).append(request[1]);
        if (request[0] == "PUT") {
            if (request.size() != 3 || request[2].empty()) {
                perror("XML Error: Received unparseable message");
                line.erase();
                continue;
            }
            if (debugger_mode) {
                cout << request[2];
            }
            checkLenght(request[1], request[2]);
            value = request[2];
//            finalRequest.append(delimiter).append(request[2]);
        } else if ((request[0] != "GET" && request[0] != "DEL") || request.size() != 2) {
            perror("XML Error: Received unparseable message");
            line.erase();
//            return errno;
//            exit(-1);
            continue;
        }
        if (debugger_mode) {
            cout << "\n";
        }


        finalRequest = toxml(request_type, key, value);
        if (debugger_mode) {
            cout << finalRequest << "\n";
            cout << "###################################### "
                    "SENDING DATA "
                    "######################################\n";
        }
        struct sockaddr_in serv_addr = {AF_INET, htons(PORT)};
        memset(&serv_addr, 0, sizeof(struct sockaddr_in));
        serv_addr.sin_family = AF_INET;
        serv_addr.sin_port = htons(PORT);
        //creating a socket
        int sockfd = socket(AF_INET, SOCK_STREAM, 0);
        if (debugger_mode) {
            cout << sockfd << "\n";
        }
        if (sockfd < 0) {
            perror("Network Error: Could not create socket");
            exit(-1);
        }
        int rc = connect(sockfd, (struct sockaddr *) &serv_addr, sizeof(serv_addr));
        if (rc < 0) {
            perror("Network Error: Could not create socket");
            exit(-1);
            //            return errno;
        }
        send(sockfd, finalRequest.c_str(), finalRequest.size(), 0);

        valread = read(sockfd, buffer1, max_buffer_size);
        close(sockfd);

        buffer1[valread] = '\0';
        std::string buffer;
        for (int i = 0; i < valread; i++) {
            buffer += (buffer1[i]);
        }
        std::string buffer2 = xmltoplain(buffer);
        char chararr_of_buffer[buffer2.length() + 1];
        strcpy(chararr_of_buffer, buffer2.c_str());
        if (debugger_mode) {
            cout << chararr_of_buffer << "\n";
        }

        if (!interactiveMode) {
            FILE *fp = fopen(argv[4], "a");
            if (!fp) {
//                return -errno;
                perror("File open error");
                exit(-1);
            }
            fprintf(fp, "%s", chararr_of_buffer);
            fprintf(fp, "\n");
            fclose(fp);
        } else {
            cout << chararr_of_buffer << std::endl;
        }
        line.erase();
    }
}