#pragma once

#include "config.h"
#include <cmath>

#define numOfTotalFDs 200
#define debugger_mode 0

#include <mutex>
#include <netdb.h>
#include <arpa/inet.h>
#include <cstring>
#include <bits/stdc++.h>
#include <cstdio>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <sstream>
#include <fstream>
#include<sys/poll.h>
#include<sys/ioctl.h>
#include <iostream>
#include <iomanip>
#include <functional>
#include <string>
#include <unordered_set>

#define True 1
#define False 0
#define delimiter "_||_"
#define max_key_lenght 256 // 256 Bytes
#define max_value_lenght (256*1024) // 256 KB
#define max_buffer_size (256 + 256*1024 + 4*2) // key_length + value_length + delimiter_length
#define number_of_bits 16
#define max_server int(pow(2,number_of_bits))

using std::cout;
using std::cin;


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

struct S {
    std::string first_name;
    std::string last_name;
};

bool operator==(const S &lhs, const S &rhs) {
    return lhs.first_name == rhs.first_name && lhs.last_name == rhs.last_name;
}

// custom hash can be a standalone function object:
struct MyHash {
    std::size_t operator()(S const &s) const noexcept {
        std::size_t h1 = std::hash<std::string>{}(s.first_name);
        std::size_t h2 = std::hash<std::string>{}(s.last_name);
        return h1 ^ (h2 << 1); // or use boost::hash_combine (see Discussion)
    }
};

namespace std {
    template<>
    struct hash<S> {
        typedef S argument_type;
        typedef std::size_t result_type;

        result_type operator()(argument_type const &s) const noexcept {
            result_type const h1(std::hash<std::string>{}(s.first_name));
            result_type const h2(std::hash<std::string>{}(s.last_name));
            return h1 ^ (h2 << 1); // or use boost::hash_combine (see Discussion)
        }
    };
}

// Used by KVStore function such as storeMapToFile and RestoreFromFile to decide which file to refer.
std::string getFilename(const std::string &key) {

    std::size_t str_hash = std::hash<std::string>{}(key);
//    std::cout << str_hash % numSetsInCache << '\n';

    int fileNumber = str_hash % numSetsInCache;

    std::string fname = "KVStore/" + std::to_string(fileNumber);

    return fname;
}

int getSetId(const std::string &key) {
    std::size_t str_hash = std::hash<std::string>{}(key);
//    std::cout << str_hash % numSetsInCache << '\n';

    int fileNumber = str_hash % numSetsInCache;
    return fileNumber;
}