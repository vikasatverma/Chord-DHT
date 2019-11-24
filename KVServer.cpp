#include "header.hpp"
#include "KVCache.cpp"
#include "KVStore.cpp"
#include "ThreadPool.h"

#include "port.cpp"
#include "functions.cpp"
#include "helperClass.cpp"
#include "nodeInformation.cpp"

int PORT;

NodeInformation nodeInfo = NodeInformation();

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


    int getServerOrKeyID() {
        std::size_t str_hash = std::hash<std::string>{}(std::to_string(PORT) + IP);

        int serverID =
                str_hash % max_server; // NOLINT(bugprone-narrowing-conversions,cppcoreguidelines-narrowing-conversions)

        return serverID;
    }


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
            if(nodeInfo.getStatus() == false){
                cout<<"Sorry this node is not in the ring\n";
            }
            else
                cout<<"Trying to put"<<key<<" "<<value<<std::endl;
            response = "Success";
        }

        else if (request_type == "DEL") {
            value = "";
            if (debugger_mode) {
                cout << "Value=" << value << "\n";
            }


            if(nodeInfo.getStatus() == false){
                cout<<"Sorry this node is not in the ring\n";
            }
            else {
                  cout<<"Trying to delete"<<key<<std::endl;
            }
            response = "Success";
        }



        else if (request_type == "GET") {
            if(nodeInfo.getStatus() == false){
                cout<<"Sorry this node is not in the ring\n";
                response="Does not exist";
            }
            else
                response="Finding";
        }

        else {
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

//            string command;
//            cout<<"Here";
//            getline(cin,command);
//            if(command=="print"){
//                nodeInfo.printKeys();
//            }
//            command.erase();
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

        nodeInfo.sp.specifyPortServer();
        PORT=nodeInfo.sp.getPortNumber();
        cout<<"Now listening at port number "<<nodeInfo.sp.getPortNumber()<<endl;


    int choice;
    while(true)
    {
        cout << "======================================\n"
                "Enter 1 to create a new ring\n"
                "Enter 2 to join some existing ring\n"
                "=========================================\n";

        cin >>choice;


        if(choice == 1)
        {
            if(nodeInfo.getStatus() == true){
                cout<<"Sorry but this node is already on the ring\n";
            }
            else{
                thread first(create,ref(nodeInfo));
                first.detach();
            }
        }
        else if(choice==2)
        {
            string ip="127.0.0.1";
            string port;
            cout << "Enter port number \n";
//            cin>>ip;
            cin>>port;
            if(nodeInfo.getStatus() == true){
                cout<<"Sorry but this node is already on the ring\n";
            }
            else
                join(nodeInfo,ip,port);

        } else if(choice==3){
            printState(nodeInfo);
        }
        else
        {
            cout << "Wrong choice enter again\n";
        }
    }



    Node node;
    Node::run();
}


