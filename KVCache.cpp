#include "header.hpp"

//TODO: use hashes instead of filenumber

class KVCache {

    std::pair<std::string, std::string> cacheMatrix[numSetsInCache][sizeOfSet];
    bool cacheReferenceMatrix[numSetsInCache][sizeOfSet]{};
    bool entryEmpty[numSetsInCache][sizeOfSet]{true};
    int lastReplacedEntry[numSetsInCache]{-1};

public:

    KVCache() {
        for (int i = 0; i < numSetsInCache; i++) {
            lastReplacedEntry[i] = -1;

            for (int j = 0; j < sizeOfSet; j++) {
                cacheReferenceMatrix[i][j] = false;
                entryEmpty[i][j] = true;
            }
        }

    }


    std::string put(const std::string &, const std::string &);

    std::string get(const std::string &);

    std::string del(const std::string &);

    int entry_to_replace(int id);

    void viewset(int);

    void cacheToXML(const std::string &filename);
};


std::string KVCache::get(const std::string &key) {
    int setID = getSetId(key);
    int not_found = 1;
    for (int i = 0; i < sizeOfSet; i++) {
        if (key == cacheMatrix[setID][i].first && !entryEmpty[setID][i]) {
            cacheReferenceMatrix[setID][i] = true;
            not_found = 0;
            return cacheMatrix[setID][i].second;
        }
    }
    if (not_found) {
        return "Does not exist";
    }
}


std::string KVCache::put(const std::string &key, const std::string &value) {
    int setID = getSetId(key);
    for (int i = 0; i < sizeOfSet; i++) {
        if (entryEmpty[setID][i]) {
            cacheMatrix[setID][i] = std::make_pair(key, value);
            entryEmpty[setID][i] = false;
            cacheReferenceMatrix[setID][i] = false;
            return "Success";
        }
    }
    int entryToReplace = entry_to_replace(setID);
    cacheMatrix[setID][entryToReplace] = std::make_pair(key, value);
    entryEmpty[setID][entryToReplace] = false;
    cacheReferenceMatrix[setID][entryToReplace] = false;
    return "Success";

}

std::string KVCache::del(const std::string &key) {
    int setID = getSetId(key);
    for (int i = 0; i < sizeOfSet; i++) {
        if (key == cacheMatrix[setID][i].first) {
            entryEmpty[setID][i] = true;
            return "Success";
        }
    }
    return "Does not exist";
}

int KVCache::entry_to_replace(int setID) {
    int i = (lastReplacedEntry[setID] + 1) % sizeOfSet;

    if (i == 0) {
        for (int j = 0; j < sizeOfSet; j++) {
            cacheReferenceMatrix[setID][j] = false;
        }
    }
    while (cacheReferenceMatrix[setID][i]) {
        i = (i + 1) % sizeOfSet;
        if (i == 0) {
            for (int j = 0; j < sizeOfSet; j++) {
                cacheReferenceMatrix[setID][j] = false;
            }
        }
    }
    lastReplacedEntry[setID] = i;
    return i;
}

void KVCache::viewset(int setID) {
    cout << "\n";
    for (int i = 0; i < sizeOfSet; i++) {
        cout << cacheMatrix[setID][i].first << cacheMatrix[setID][i].second << std::endl;
    }
}

void KVCache::cacheToXML(const std::string &filename) {
    std::string cacheXML = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n";
    cacheXML += "<KVCache>\n";

    for (int i = 0; i < numSetsInCache; i++) {
        cacheXML += " <Set Id=\"" + std::to_string(i) + "\">\n";
        for (int j = 0; j < sizeOfSet; j++) {
            std::string isReferenced;
            std::string isValid;
            if (cacheReferenceMatrix[i][j]) {
                isReferenced = "true";
            } else
                isReferenced = "false";

            if (entryEmpty[i][j]) {
                isValid = "false";
            } else
                isValid = "true";

            cacheXML += "  <CacheEntry isReferenced=\"" + isReferenced + "\"";
            cacheXML += " isValid=\"" + isValid + "\">\n";
            std::string key = cacheMatrix[i][j].first;
            std::string value = cacheMatrix[i][j].second;
            cacheXML += "   <Key>" + key + "</Key>\n";
            cacheXML += "   <Value>" + key + "</Value>\n";
            cacheXML += "  </CacheEntry>\n";
        }
        cacheXML += " </Set>\n";
    }
    cacheXML += "</KVCache>\n";

    char chararr_of_XML[cacheXML.length() + 1];
    strcpy(chararr_of_XML, cacheXML.c_str());
    FILE *fp = fopen(filename.c_str(), "w");

    fprintf(fp, "%s", chararr_of_XML);
    fprintf(fp, "\n");

    fclose(fp);
}
