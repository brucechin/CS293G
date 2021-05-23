#include <cassert>
#include <cmath>
#include <cstring>
#include <fstream>
#include <iostream>
#include <limits>
#include <vector>
#include <map>
#include <tuple>
#include <algorithm>    // std::min
#include <random>
#include<thread>
#include <chrono>
#define FILE_MAX 1000000


inline uint32_t hash_str1(const std::string& str) {

    uint32_t hash = 0x811c9dc5;
    uint32_t prime = 0x1000193;

    for(int i = 0; i < str.size(); ++i) {
        uint8_t value = str[i];
        hash = hash ^ value;
        hash *= prime;
    }

    return hash;

}


inline uint32_t hash_str2(const std::string& str) {

    uint32_t hash = 0x81a6c9dc;
    uint32_t prime = 0x1000193;

    for(int i = 0; i < str.size(); ++i) {
        uint8_t value = str[i];
        hash = hash ^ value;
        hash *= prime;
    }

    return hash;

}


inline uint32_t hash_str3(const std::string& str) {

    uint32_t hash = 0x159c9425;
    uint32_t prime = 0x1000193;

    for(int i = 0; i < str.size(); ++i) {
        uint8_t value = str[i];
        hash = hash ^ value;
        hash *= prime;
    }

    return hash;

}

inline uint8_t encrypt(uint8_t plaintext){
    uint8_t key = 0x12;
    return plaintext ^ key;
}

inline uint8_t decrypt(uint8_t ciphertext){
    uint8_t key = 0x12;
    return ciphertext ^ key;
}


bool sortHelper(std::tuple<uint32_t, uint8_t> a, std::tuple<uint32_t, uint8_t> b){
    return std::get<1>(a) > std::get<1>(b);
}

class EncryptSearchIndex{

    private:
        std::vector<std::vector<uint8_t>> m_filter;
        uint32_t m_hashNum = 3;
        uint32_t fileNum = FILE_MAX;
        uint32_t bloomFilterSize = 2048; //number of slots for each file.
    public:
        EncryptSearchIndex()
        {
            for(uint32_t i = 0; i < bloomFilterSize; i++){
                m_filter.push_back(std::vector<uint8_t>(fileNum));
                //printf("index allocated %d size is %d\n", i, m_filter[i].size());
            }
            
        }
        EncryptSearchIndex(uint32_t fileCapacity, uint32_t blmFilterSize)
        {
            fileNum = fileCapacity;
            bloomFilterSize = blmFilterSize;
            for(int i = 0; i < bloomFilterSize; i++){
                m_filter.push_back(std::vector<uint8_t>(fileNum));
            }
        }

        ~EncryptSearchIndex(){
        }

        //<hash1, hash2, hash3, count>
        bool insertFile(uint32_t fileIndex, std::vector<std::vector<uint32_t>> keywordsCount){
            for(int i = 0; i < keywordsCount.size(); i++){
                for(int j = 0; j < m_hashNum; j++){
                    uint8_t cnt = keywordsCount[i][m_hashNum];
                    m_filter[keywordsCount[i][j] % bloomFilterSize][fileIndex] = cnt; //the last element is the count
                    //printf("count %d\n", m_filter[keywordsCount[i][j] % bloomFilterSize][fileIndex]);
                }
            }
        }

        bool removeFile(uint32_t fileIndex){
            //reset the counter slots all to zero for the target file index
            for(int i = 0; i < bloomFilterSize; i++){
                m_filter[i][fileIndex] = 0;
            }
        }


        std::vector<std::vector<uint8_t>> queryOneKeyword(std::vector<uint32_t> hashes){

            std::vector<std::vector<uint8_t>> result;
            for(int i = 0; i < hashes.size(); i++){
                //printf("bit %d\n", hashes[i] % bloomFilterSize);
                result.push_back(m_filter[hashes[i] % bloomFilterSize]);
                // for(int j = 0; j < fileNum; j++){
                //     printf("m_filter %d\n", m_filter[hashes[i] % bloomFilterSize][j]);
                // }
            }
            return result;
        }

        void printFilter(){
            for(int i = 0; i < bloomFilterSize; i++){
                for(int j = 0; j < fileNum; j++){
                    printf("%d ", m_filter[i][j]);
                }
                printf("\n");
            }
        }

};


class Client{
    public:

        EncryptSearchIndex index;
        uint32_t serverFileCapacity = FILE_MAX;
        

    Client(EncryptSearchIndex& index){
        index = index;
    }


    bool insertFile(uint32_t fileIndex, std::map<std::string, uint32_t> keywordsCount){
        std::vector<std::vector<uint32_t>> keywordsToInsert;
        std::map<std::string, uint32_t>::iterator it;
        for(it = keywordsCount.begin(); it != keywordsCount.end();  it++){
            std::string keyword = it->first;
            uint8_t count = encrypt(it->second);
            //printf("count %d\n", count);
            uint32_t hash1 = hash_str1(keyword);
            uint32_t hash2 = hash_str2(keyword);
            uint32_t hash3 = hash_str3(keyword);
            std::vector<uint32_t> tmp = {hash1, hash2, hash3, count};
            //printf("count %d\n", tmp[3]);

            keywordsToInsert.push_back(tmp);
        }
        index.insertFile(fileIndex, keywordsToInsert);
    }

    std::vector<uint32_t> queryOneKeyword(std::string keyword, uint32_t topK){ //return the top K file indexes ordered by the keyword frequency.
        
        uint32_t hash1 = hash_str1(keyword);
        uint32_t hash2 = hash_str2(keyword);
        uint32_t hash3 = hash_str3(keyword);
        std::vector<uint32_t> hashes = {hash1, hash2, hash3};
        std::vector<std::vector<uint8_t>> bloom = index.queryOneKeyword(hashes);
        std::vector<std::tuple<uint32_t, uint8_t>> counter;
        for(uint32_t i = 0; i < serverFileCapacity; i++){
            uint8_t frequency = std::min(decrypt(bloom[0][i]), decrypt(bloom[1][i]));
            frequency = std::min(frequency, decrypt(bloom[2][i]));
            //printf("%d %d %d\n", bloom[0][i], bloom[1][i], bloom[2][i]);
            counter.push_back(std::make_tuple(i, frequency));
        }
        sort(counter.begin(), counter.end(), sortHelper); // sort based on frequency

        std::vector<uint32_t> fileIndexesToFetch;
        for(int i = 0; i < topK; i++){
            fileIndexesToFetch.push_back(std::get<0>(counter[i])); 
        }
        return fileIndexesToFetch;
        //construct PIR query vector based on fileIndexesToFetch, which is out of scope for this project.
    }
};

void initServerIndex(Client& client){
    std::vector<std::string> keywords = {"hello", "world", "push", "back","sort","based","out","in","scope","project","vector","PIR","harry","potter","construct","update","insert","delete","thank","norm","query","hash","counter","frequency", "file"};
    int avgKeywordsPerFile = 5;
    for(int i = 0; i < client.serverFileCapacity; i++){
        std::map<std::string, uint32_t> toInsertKeywordsCounter;
        //printf("init step %d\n", i);
        for(int j = 0; j < avgKeywordsPerFile; j++){
            std::string keyword = keywords[std::rand() % keywords.size()];
            uint32_t frequency = std::rand() % 256;
            toInsertKeywordsCounter[keyword] = frequency;

        }
        client.insertFile(i, toInsertKeywordsCounter);
    }
    //client.index.printFilter();
}

void parallelBench(){
    std::vector<std::string> keywords = {"hello", "world", "push", "back","sort","based","out","in","scope","project","vector","PIR","harry",
                                        "potter","construct","update","insert","delete","thank","norm","query","hash","counter","frequency", "file",
                                        "animal", "species", "dream", "drink","driver","food","drug","earth","ease","economic","export","expand","failure"};

    EncryptSearchIndex index;
    Client client(index);
    initServerIndex(client);
    int threadNum = 128; 

    for(int k = 1; k <= 128; k*=2){
        threadNum = k;
        std::vector<std::thread> threads;
        auto startTime = std::chrono::system_clock::now();                                           
        for(int i = 0; i < threadNum; i++){
            threads.push_back(std::thread(&Client::queryOneKeyword, &client, keywords[i % keywords.size()], 5));
        }
        for(int i = 0; i < threadNum; i++){
            threads[i].join();
        }
        auto endTime = std::chrono::system_clock::now();                                             
        std::chrono::duration<double> elapsedSeconds = endTime - startTime;                                
        std::cout << ">>>  " << k << " missions completed in " << elapsedSeconds.count() << " seconds.\n"; 
    }
}

int main(int argc, char **argv){

    parallelBench();

}