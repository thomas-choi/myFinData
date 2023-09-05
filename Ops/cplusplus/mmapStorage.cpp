#include <iostream>
// #include <fstream>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>

#include    "mmapStorage.h"

mmapStorage::mmapStorage(string filename, int   initSize) {
    cout << "mmapStorage::mmapStorage(" << filename << ")\n";
    // Open the file and get its size
    int     fd = open("data.txt", O_RDWR|O_CREAT, (S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP));
    if (fd == -1) {
        perror("Could not open file");
        return;
    }
    mmapStorage::_size = lseek(fd, 0, SEEK_END);
    if (mmapStorage::_size <= 0) {
        ftruncate(fd, initSize);
        mmapStorage::_size = lseek(fd, 0, SEEK_END);
    }

    // Map the file into memory
    cout << "File size is " << mmapStorage::_size << std::endl ;
    mmapStorage::_mmapPtr = (char*) mmap(NULL, mmapStorage::_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (mmapStorage::_mmapPtr == MAP_FAILED) {
        perror("Could not mmap file");
        return;
    }
    close(fd);                         // Step 6 close
}

void        mmapStorage::sync() {
   msync(mmapStorage::_mmapPtr, mmapStorage::_size, MS_ASYNC);   
}

mmapStorage::~mmapStorage() {
    cout << "mmapStorage::~mmapStorage()\n";
    msync(mmapStorage::_mmapPtr, mmapStorage::_size, MS_SYNC);       // Step 4 Sync memory to file
    munmap(mmapStorage::_mmapPtr, mmapStorage::_size);               // Step 5 unmapping
}
