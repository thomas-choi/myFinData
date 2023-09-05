//
// main.cpp - This file is part of the Grinder library
//
// Copyright (c) 2015 Matthew Brush <mbrush@codebrainz.ca>
// All rights reserved.
//

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <poll.h>

#include <Grinder/Grinder>
#include <iostream>
#include <stdio.h>
#include <unistd.h>

#include <boost/program_options.hpp>

#include <sstream>
#include <vector>
#include <fcntl.h>
#include <errno.h>

#include <thread>
#include <semaphore>
#include <fcntl.h>
#include <cstdio>
#include    <chrono>
#include    <ctime>
#include    <string.h>

#include "UNQueue.h"

using namespace std;
using namespace Grinder;

namespace po = boost::program_options;

const	char *init_command = "open|%s|%s|mode|both|\n";
const	char *init_SPY = "open|XBI|XBI|mode|both|\n";
const	char *hb_sym="heartbeat";

static const	int		_tReadsize = 1024*30;
static const	int		_tWritesize = 1024*30;
char			_treadbuf[_tReadsize];
char			_twritebuf[_tWritesize];

int				_debug = 0;

static	int		_InCount = 0;

UNQueue		myQ;

int		sfd = 0;
int		ofd = 1;

void	consumerThread(int fd, EventLoop *loop) {
	int		thisSize;
	int		c_bytes=0;

	if (_debug>0)
		printf("C* Start consumerThread with %d\n", fd);
	memset(&_twritebuf[0], 0, _tWritesize);
	while ((thisSize=myQ.PopOutQueue(&_twritebuf[0], _tWritesize)) > 0) {		// 
		 // try to conumer all until c_bytes is thisSize
		c_bytes = write(fd, &_twritebuf[0], thisSize);
		if (_debug>1)
			printf("C*   write file=%d\n", c_bytes);
		if (c_bytes < 0) {
			perror("Failed to consume from buf");
			return ;
		}
		if (_debug>1)
			printf("C* consume: %d bytes, %s\n", c_bytes, myQ.to_string());
		if ((strcmp("$$$$", _twritebuf)==0)) {
			fprintf(stderr, "consumer stop loop\n");
			loop->quit();
			return ;
		}
	}
}

int	ReadInQueue(int fd) {
	memset(&_treadbuf[0], 0, _tReadsize);
	int 	rd_bytes = recv(fd, &_treadbuf[0], _tReadsize, 0);

	if (rd_bytes == -1) {
		char	buf[1024];
		sprintf(buf, "Failed to produce from fd:%d\n", rd_bytes);
		perror(buf);
		return rd_bytes;
	} 
	if (_debug>1)
		printf("R*  bytes(%d)\n", rd_bytes);
	int		byte_cnt = 0, putSize, sleep_cnt=0;
	while ((rd_bytes-byte_cnt)>0) {
		if ((putSize=myQ.PutInQueue(&_treadbuf[byte_cnt], min(rd_bytes-byte_cnt, _tReadsize))) > 0) {
			byte_cnt += putSize;
			if (_debug>1)
				printf("Produce: %d rd_bytes, %s, byte_c=%d, putSize=%d\n", 
					rd_bytes, myQ.to_string(), byte_cnt, putSize);
		} else {
			sleep_cnt +=1;
			int		usec = 10*pow(2,sleep_cnt);
			if (_debug>0) {
				printf("  R* Sleep usec:%d\n", usec);
			}
			usleep(usec);		// sleep to wait for consumer to pop out more space from buffer
		}

	}
	return rd_bytes;
}

vector<string>	ParseList(string instr) {

	vector<string> tokens;
	stringstream	ss(instr);
	string 			token;
	while (getline(ss, token, ',')) {
		tokens.push_back(token);
	}
	  // Print the tokens
  	printf("The tokens are:\n");
  	for (const auto& t : tokens) {
    	cout << t << endl;
  	}

  	return tokens;
}

int makeDDSConnect(const char *IPaddress, int port)
{
	fprintf(stderr, "makdDDSConnect: %s: %d\n", IPaddress, port);

	if (strlen(IPaddress)==0 || port<=0) {
		return -1;
	}
	// Create a socket
	int sockfd = socket(AF_INET, SOCK_STREAM, 0);
	if (sockfd == -1)
	{
		perror("Failed to create socket");
		return -1;
	}

	// Define the server address
	struct sockaddr_in serv_addr;
	serv_addr.sin_family = AF_INET;
	serv_addr.sin_port = htons(port);				// Port number
	serv_addr.sin_addr.s_addr = inet_addr(IPaddress); // IP address

	// Connect to the server
	if (connect(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) == -1) {
		perror("Failed to connect to server");
		return -1;
	}

	int flags = fcntl (sockfd, F_GETFL);
	if (flags < 0) {
		fprintf(stderr, "could not get file flags");
	} else {
		if (fcntl (sockfd, F_SETFL, flags | O_NONBLOCK) < 0) {
		 	perror("could not set file flags");
		}
	}
	printf("Connected to server with %d.\n", sockfd);
	return sockfd;
}


int	InitDownload(int  fd, vector<string>	&symlist) {
	if (fd > 2) {
		for (const auto& t : symlist) {
			char 	buf[1024];
			buf[0] = 0;
			sprintf(buf, init_command, t.c_str(), t.c_str());
			printf("%s", buf);
			if (fd > 0) {
				int wr_bytes = write(fd, buf, strlen(buf));
				if (wr_bytes == -1) {
					char	buf[1024];
					sprintf(buf, "Failed to write to : %d", fd);
					perror(buf);
					return -1;
				}
			}
		}
	} else {
		fprintf(stderr, "fd must be greater 2: %d\n", fd);
		return -1;
	}
	return 0;
}

bool    ddsHandler(EventSource &) {
	if (_debug>0)
		printf("FILE input: (%d) event\n", sfd);
	int		rd = ReadInQueue(sfd);
	_InCount=0;
	if (rd == -1) {
		return false;
	}
	return true; 		
}

void	initTimeUtil() {
    if (strcmp("EST", tzname[0])) {
		if (_debug>0) {
			printf("Set to EST tzone.\n");
		}
        setenv("TZ", "US/Eastern", 1);
        tzset();
    }
}

int		timeFactor(struct tm *tm) {
	if (_debug>10) {
		printf("timeFactor: %d, %s\n", tm->tm_hour, tm->tm_zone);
	}
	if (((tm->tm_hour>16)||(tm->tm_hour<9)) && (strcmp("EDT", tm->tm_zone)==0))
		// market is closed
		return 10;
	else
		return 2;
}

int main(int argc, char* argv[])
{
	initTimeUtil();
	  // Define the options
	po::options_description desc("Allowed options");
	desc.add_options()
		("help", "produce help message")
		("IP", po::value<string>(), "IP address of remote server")
		("port", po::value<int>(), "port of remote server")
		("debug", po::value<int>()->default_value(1), "port of remote server")
		("symlist", po::value<string>(), "symbol list")
		("outf", po::value<string>(), "output file");

	po::variables_map vm;
	po::store(po::parse_command_line(argc, argv, desc), vm);
	po::notify(vm);    

	string	outpath;
	string	ip = "";
	int		port = 0;

	// Access the options
	if (vm.count("help")) {
		cout << desc << "\n";
		return 1;
	}
	if (vm.count("IP")) {
		ip = vm["IP"].as<string>();
		cout << "IP address: " << ip << "\n";
	}
	if (vm.count("port")) {
		port = vm["port"].as<int>();
		cout << "Port: " <<  port << "\n";
	}
	if (vm.count("symlist")) {
		cout << "Symbol list: " << vm["symlist"].as<string>() << "\n";
	}
	if (vm.count("debug")) {
		_debug = vm["debug"].as<int>();
		cout << "Port: " <<  port << "\n";
		myQ.setDebug(_debug);
	}
	if (vm.count("outf")) {
		outpath = vm["outf"].as<string>();
		cout << "Out Dump File: " << outpath << "\n";
	}
	vector<string>	symbols = ParseList( vm["symlist"].as<string>());

	EventLoop loop;

	printf("%s starts:   queue size:%d, Readbuf:%d, Writebuf:%d\n", 
			argv[0], _bufsize, _tReadsize, _tWritesize);
	sfd = makeDDSConnect(ip.c_str(), port);
	if (InitDownload(sfd, symbols) < 0) 
		return -1;

	if (outpath.length() > 0) { 
		ofd = open(outpath.c_str(), (O_RDWR | O_APPEND | O_CREAT | O_SYNC ), S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
		printf("Setup Testing out fd %d\n", ofd);
	}



	loop.add_file(sfd, FileEvents::INPUT, ddsHandler);

	// loop.add_file(sfd, FileEvents::INPUT, [&](EventSource &) {
	// 	if (_debug>0)
	// 		printf("FILE input: (%d) event\n", sfd);
	// 	int		rd = ReadInQueue(sfd);
	// 	_InCount=0;
	// 	if (rd == -1) {
	// 		return false;
	// 	}
	// 	return true; 
	// });

	auto sig_source = new GenericSignalSource(true);
	sig_source->add(SIGINT);
	sig_source->add(SIGTERM);
	loop.add_event_source(sig_source, [&](EventSource &) {
		printf("Recevied signal \'%d\', quitting\n", sig_source->signo);
		loop.quit();
		return true; 
	});

	loop.add_timeout(2000, [&](EventSource&) {
		time_t  rtime = std::time(0);
		struct tm *ptm = localtime(&rtime);
		int		factor = timeFactor(ptm);

		if (++_InCount>(factor*2)) {
			printf("  * InCount:%d\n", _InCount);
		}
		if (_InCount>(factor*3)) {
			printf(" * Reconnect socket %d.\n", sfd);
			_InCount=0;
			if (sfd>0) {
				printf(" SHUTDOWN socket:%d\n", sfd);
				if (shutdown(sfd, SHUT_RDWR)<0)
					perror("Shutdown fd failed");	
				close(sfd);
			}
			sfd = makeDDSConnect(ip.c_str(), port);
			fprintf(stderr, "Setup Testing in fd %d\n", sfd);
			InitDownload(sfd, symbols);
			loop.add_file(sfd, FileEvents::INPUT, ddsHandler);
		}
		
		fflush(stdout);
		fflush(stderr);
		return true;
	});

	std::thread		t2(consumerThread, ofd, &loop);

	printf("> Start Loop.run\n");
	return loop.run();
}
