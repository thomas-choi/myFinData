#ifndef MARKETIMAGE_H
#define MARKETIMAGE_H

#include "mmapStorage.h"
#include "Ticker.h"

class MarketImage : mmapStorage
{
protected:
    struct header_rec {
        char ItemSize[10];
        char ItemLimit[10];
        char ItemCount[10];
        char space[70];
    }       *header=0;      // Header is 100 bytes:   
    Ticker  *tickers=0;     // array of Ticker
    int     ticker_limit=100;
    int     ticker_count=0;

    void    SetUp() {
        MarketImage::header = (struct header_rec *)mmapStorage::_mmapPtr;
        MarketImage::tickers = (Ticker*)(mmapStorage::_mmapPtr+100);
    };

public:

static const int   initSize = 100+10*sizeof(Ticker);

    MarketImage(string fname);

    void        write(string line);
    string      read();
};


#endif