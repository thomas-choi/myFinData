#ifndef TICKER_H
#define TICKER_H

#include <string>
#include <iostream>
#include <cstring>

class Ticker 
{
    char    symbol[21];
    char    timestamp[11];
    char    currency[5];
    char    name[21];
    float   volume;
    float   last;
    float   open;
    float   high;
    float   low;
    float   close;
    float   pclose;
    float   ask;
    float   bid;
    float   askvol;
    float   bidvol;
    float   tradeAmount;

    const   std::string     delimiter="|";
 
protected:

    void    scanline(std::string s);

public:

    void    reset();   
    void    write(std::string s) {
        reset();
        scanline(s);
    };
    void    update(std::string s) {
        scanline(s);
    };
};

#endif