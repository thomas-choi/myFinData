#ifndef		UNQUEUE_H
#define		UNQUEUE_H

// #include <iostream>
// #include <stdio.h>
// #include <cstring>
#include <unistd.h>
#include <semaphore>

// using namespace std;

static const	int		_bufsize = 1024*100;

class UNQueue {
private:
    std::counting_semaphore<1>  *sem; // create a semaphore with a limit of 2 and an initial value of 0
    char			_sharedbuf[_bufsize];
    int				_pptr = 0;				// pointer to first byte for write or put
    int				_cptr = -1;		// pointer to first byte for read or pop		
	int				_debug;

	public:

    UNQueue(int dbugf=0);

	const char*	to_string();

	inline 	void	setDebug(int dbugf) { _debug = dbugf; }

	inline	int		IsQueueEmpty() {
		int		ret=0;
		if (((_pptr>_cptr) && (_pptr-_cptr-1)==0) 
			|| (_pptr == _cptr)) {
			ret =  1;
		}
		if (_debug>1)
			printf(" IsQueuEmpty=%d(p=%d, c=%d)\n", ret, _pptr, _cptr);
		return ret;
	}

	inline int 		allocPutSize() {
		int		thisSize = _bufsize - _pptr;
		if (_cptr > _pptr) {
			thisSize = _cptr - _pptr;
		}
		if (_debug>1)
			printf(" allocPutSize=%d(p=%d, c=%d)\n", thisSize, _pptr, _cptr);
		return thisSize;
	}

	inline	int		allocPopSize() {
		int		thisSize = _pptr - _cptr;
		if (IsQueueEmpty()) {
			thisSize -= 1;
		} else if (_cptr > _pptr) {
			thisSize = _bufsize - _cptr;
		} 
		if (_debug>1)
			printf(" allocPopSize=%d(p=%d, c=%d)\n", thisSize, _pptr, _cptr);
		return thisSize;
	}

	int	PutInQueue(char * data, int datasize);

// always wait until something in the queue
	int	PopOutQueue(char * buf, int bufsize);

};

#endif