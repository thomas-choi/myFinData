#include <iostream>
#include <stdio.h>
#include <cstring>
#include <unistd.h>
#include <semaphore>

#include "UNQueue.h"

using namespace std;

UNQueue::UNQueue(int dbugf) {
    sem = new std::counting_semaphore<1>(0);
    _debug = dbugf;
}

const char*	UNQueue::to_string() {
static char		buf[1024];
	sprintf(buf, "p=%d, c=%d", _pptr, _cptr);
	return buf;
}


int	UNQueue::PutInQueue(char * data, int datasize) {
	int		bsize =min(allocPutSize(), datasize);
	if (_debug>1)
		printf("PutInQueue=%d(datasize=%d)\n", bsize, datasize);
	if (bsize > 0) {
		memcpy(&_sharedbuf[_pptr], data, bsize);
		int   prev = IsQueueEmpty();
		_pptr += bsize;
		// wrap arount the publish ptr to beginning of buffer
		if ((_pptr==_bufsize) && (_cptr>0)) {		
			_pptr = 0;
		}
		if (prev) {
			if (_cptr == -1)
				_cptr = 0;
			if (_debug>0)
				printf("Release sem\n");
			sem->release();
		}
	}
	return bsize;
}

// always wait until something in the queue
int	UNQueue::PopOutQueue(char * buf, int bufsize) {
	AGAIN:
	int		bsize = min(allocPopSize(), bufsize);
	if (_debug>1)
		printf("PopOutQueue=%d(bufsize=%d, c=%d, p=%d\n",  bsize, bufsize, _cptr, _pptr);

	if (bsize > 0) {
		memcpy(buf, &_sharedbuf[_cptr], bsize);
		memset(&_sharedbuf[_cptr], 0, bsize);
		buf[bsize]=0;
		if (_debug>5) {
			printf("PopQ>%s<\n", buf);
		}
		_cptr += bsize;
		if (_cptr >= _bufsize) {
			_cptr = -1;
			if (_pptr >= _bufsize) {
				_pptr = 0;
			} else if (_pptr > 0) {
				_cptr = 0;
			}
		}
	} else {
		if (_debug>0)
			printf("Acquire Sem by size(%d)\n", bsize);
		sem->acquire();
		if (_debug>0)
			printf("Sem awoke\n");
		goto AGAIN;
	}
	return bsize;
}
