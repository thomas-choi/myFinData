// Include the library header 
#include <iostream>
#include <stdio.h>
#include <nlohmann/json.hpp> 
#include <fstream>

#include "MarketImage.h"

// Use the namespace alias for convenience 
using json = nlohmann::json;

// Define the input string 
std::string input = (const char*)"timestamp|10:30:10|ticker|IBM|close|113.45|";
std::string  True=(const char*)"true";
std::string  False=(const char*)"false";

// Create an empty JSON object 
json output;

// Split the input string by the delimiter character ‘|’ 
std::stringstream   ss(input); 
std::string         token; 

int JsonTest()
{
    bool is_key = true; 
    while (std::getline(ss, token, '|')) { 
        // Assume that every odd token is a key and every even token is a value static 
        static std::string key; 
        if (is_key) { 
            // Store the key for later use 
            key = token; 
        } else { 
            // Convert the value to the appropriate type and store it in the JSON object 
            // For simplicity, we only handle strings, numbers, and booleans here 
            if (token == True || token == False) { 
                // Convert to boolean 
                output[key] = (token == True); 
            } else if (token.find('.') != std::string::npos) { 
                    // Convert to double 
                    output[key] = std::stod(token); 
            } else { 
                // Try to convert to integer, otherwise use string 
                try { 
                    output[key] = std::stoi(token); 
                } catch (const std::invalid_argument&) { 
                    output[key] = token; 
                }
            }
        }
        // Toggle the flag for the next token 
        is_key = !is_key; 
    }

    // Print the JSON object 
    std::cout << output.dump(4) << std::endl;
    return 0;
}

class MIHeader {
    char    *_buffer;

public:
    MIHeader(char *ePtr) {
        MIHeader::_buffer = ePtr;
    };
    int     ticker_size = sizeof(Ticker);
    int     ticker_limit=100;
    int     ticker_count=0;
};

MarketImage::MarketImage(string fname):mmapStorage(fname, MarketImage::initSize) {
    cout << "MarketImage::MarketImage(" << fname << ")\n";
}

void     MarketImage::write(string line) {
    memcpy(mmapStorage::_mmapPtr, line.c_str(), line.length());
    mmapStorage::sync();
}

string     MarketImage::read() {
    return mmapStorage::_mmapPtr;
}