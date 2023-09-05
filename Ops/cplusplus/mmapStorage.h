#ifndef MMAPSTORAGE_H
#define MMA

#include    <string>

using namespace std;

class mmapStorage {

private:
    size_t  _size;

protected:
    char    *_mmapPtr = NULL;
    void    sync();

public:
    mmapStorage(string filename, int initSize);
    ~mmapStorage();

};

#endif