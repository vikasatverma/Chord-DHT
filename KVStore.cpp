#include "header.hpp"

std::mutex DumpToFile;
std::mutex restoreFromFile;
std::mutex mapToFile;
std::mutex addToFile;


class KVStore {

public:
    int dumpToFile(const std::string &filename) {
        DumpToFile.lock();
        FILE *dumpFilePtr = fopen(filename.c_str(), "w");
        if (!dumpFilePtr) {
            DumpToFile.unlock();

            return -errno;
        }
        fprintf(dumpFilePtr, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<KVStore>\n");
        for (int i = 0; i < numSetsInCache; i++) {
            std::string nameStoreFile = "KVStore/" + std::to_string(i);

            FILE *storeFilePtr = fopen(nameStoreFile.c_str(), "r");
            if (!storeFilePtr)
                continue;

            char *buf = nullptr;
            size_t buflen = 0;
            while (getline(&buf, &buflen, storeFilePtr) > 0) {
                char *nl = strchr(buf, '\n');
                if (nl == nullptr)
                    continue;
                *nl = 0;

                char *sep = strchr(buf, '=');
                if (sep == nullptr)
                    continue;
                *sep = 0;
                sep++;

                std::string key = buf;
                std::string value = sep;

                fprintf(dumpFilePtr, " <KVPair>\n");
                fprintf(dumpFilePtr, "%s", ("  <Key>" + key + "</Key>\n").c_str());
                fprintf(dumpFilePtr, "%s", ("  <Value>" + value + "</Value>\n").c_str());
                fprintf(dumpFilePtr, " </KVPair>\n");

            }
            fclose(storeFilePtr);
        }

        fprintf(dumpFilePtr, "</KVStore>\n");

        fclose(dumpFilePtr);

        DumpToFile.unlock();
        return 0;
    }

    int RestoreFromFile(const std::string &filename) {
        restoreFromFile.lock();
        std::vector<FILE *> fd_vector;

        for (int i = 0; i < numSetsInCache; i++) {
            std::string fname = "KVStore/" + std::to_string(i);
            FILE *fp = fopen(fname.c_str(), "a");
            if (!fp) {
                cout << "IO Error";
                restoreFromFile.unlock();

                return -errno;
            }
            fd_vector.push_back(fp);
        }

        FILE *sourceFile = fopen(filename.c_str(), "r");
        if (!sourceFile) {
            restoreFromFile.unlock();

            return -errno;
        }
        char *buf = nullptr;
        size_t buflen = 0;
        getline(&buf, &buflen, sourceFile);
        getline(&buf, &buflen, sourceFile);
        getline(&buf, &buflen, sourceFile);

        while (getline(&buf, &buflen, sourceFile) > 0) {

            std::string key;
            std::string value;

            std::string line = std::string(buf);
            if (buf[3] == 'K' && buf[4] == 'e' && buf[5] == 'y') {
                std::size_t start_of_key = line.find("<Key>");
                std::size_t end_of_key = line.find_last_of("</Key>");
                key = line.substr(start_of_key + 5, line.length() - 14);
            }
            getline(&buf, &buflen, sourceFile);
            line = std::string(buf);

            if (buf[3] == 'V' && buf[4] == 'a' && buf[5] == 'l') {
                std::size_t start_of_Value = line.find("<Value>");
                std::size_t end_of_Value = line.find_last_of("</Value>");
                value = line.substr(start_of_Value + 7, line.length() - 18);
            }

            getline(&buf, &buflen, sourceFile);
            getline(&buf, &buflen, sourceFile);

            FILE *fp = fd_vector[getSetId(key)];
//            cout << key << value << std::endl;
            fprintf(fp, "%s=%s\n", key.c_str(), value.c_str());


        }
        fclose(sourceFile);

        for (int i = 0; i < numSetsInCache; i++) {

            fclose(fd_vector[i]);
        }
        restoreFromFile.unlock();
        return 0;
    }


};


// To be used to access key value pairs when not found in cache
// this will load the required file into the temporary map ie m
int populateMap(std::string &key, std::map<std::string, std::string> *m) {
    std::string fname = getFilename(key);
    int count = 0;
    if (access(fname.c_str(), R_OK) < 0)
        return -errno;

    FILE *fp = fopen(fname.c_str(), "r");
    if (!fp)
        return -errno;

    m->clear();

    char *buf = nullptr;
    size_t buflen = 0;
    while (getline(&buf, &buflen, fp) > 0) {
        char *nl = strchr(buf, '\n');
        if (nl == nullptr)
            continue;
        *nl = 0;

        char *sep = strchr(buf, '=');
        if (sep == nullptr)
            continue;
        *sep = 0;
        sep++;

        std::string s1 = buf;
        std::string s2 = sep;

        (*m)[s1] = s2;

        count++;
    }

    if (buf)
        free(buf);

    fclose(fp);
    return count;
}

// Rewrites the whole file in case of a delete
int storeMapToFile(std::string &key, std::map<std::string, std::string> *m) {
    mapToFile.lock();
    std::string fname = getFilename(key);


    FILE *fp = fopen(fname.c_str(), "w");
    if (!fp) {
        return -errno;
    }
    int count = 0;
    if (m->empty()) {
        fclose(fp);
        mapToFile.unlock();

        return 0;
    }

    for (std::map<std::string, std::string>::iterator it = m->begin(); it != m->end(); it++) {
        fprintf(fp, "%s=%s\n", it->first.c_str(), it->second.c_str());
        count++;
    }

//    std::cout << count;
    fclose(fp);
    mapToFile.unlock();
    return count;
}


// Inserts key-value pair incrementally
int putIntoFile(std::string &key, std::string &value) {
    addToFile.lock();
    std::string fname = getFilename(key);


    FILE *fp = fopen(fname.c_str(), "a");
    if (!fp) {
        addToFile.unlock();
        return -errno;
    }
    fprintf(fp, "%s=%s\n", key.c_str(), value.c_str());

    fclose(fp);
    addToFile.unlock();
    return 0;
}

std::string toXML(std::string str) {
    std::string response, key, value;
    std::string header = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n";
    std::string msg = "<KVMessage type=\"resp\">\n";
    if (debugger_mode) {
        cout << "\nstr is =>" << str << "<=\n" << std::endl;
    }
    if (str == "Success" || str == "Error Message" || str == "Does not exist")
        msg = msg + "<Message>" + str + "</Message>\n";
    else {
        for (auto i = 0; i < str.length(); i++) {
            if (str[i] != ' ')
                key += str[i];
            else {
                value = str.substr(i + 1);
                break;
            }

        }

        msg = msg + "<Key>" + key + "</Key>\n" + "<Value>" + value + "</Value>\n";
    }
    response = header + msg + "</KVMessage>\n";
    return response;
}


//convert xml format to plain text
std::string fromxml(std::string str) {
    std::string request_type;
    std::string msg_type = str.substr(56, 6);
    std::string key;
    std::string value;
    int i = 0, j = 0;
    if (msg_type == "putreq") {
        request_type = "PUT";
        for (i = 70; str[i] != '<'; i++) {
            key += str[i];
        }
        j = i + 14;
        for (; str[j] != '<'; j++) {
            value += str[j];
        }
        key = key + delimiter + value;
    } else if (msg_type == "getreq") {
        request_type = "GET";
        for (i = 70; str[i] != '<'; i++) {
            key += str[i];
        }
    } else {
        request_type = "DEL";
        for (i = 70; str[i] != '<'; i++) {
            key += str[i];
        }
    }
    request_type = request_type + delimiter + key;
    return request_type;
}


//int main() {
//
//    KVStore KS;
//    KS.RestoreFromFile("testfile.xml");
////    sleep(10);
//    KS.dumpToFile("testfile.xml");
//
//}