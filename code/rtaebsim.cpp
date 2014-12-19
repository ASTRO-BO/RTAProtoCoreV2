
#include <zmq.h>
#include <zmq_utils.h>

#include <cstdlib>
#include <cstdio>
#include <unistd.h>
#include <iostream>
#include <iomanip>

#include <ctime>
#include "mac_clock_gettime.h"

#include <packet/PacketBufferV.h>

using namespace PacketLib;

using namespace std;

struct timespec start, stop;
unsigned long totbytes;
unsigned long sumtotbytes;
unsigned long npacketssent;
unsigned long npacketssent_thread;
unsigned long sumnpacketssent;

void end(int ntimefilesize=1) {
	clock_gettime( CLOCK_MONOTONIC, &stop);
	double time = timediff(start, stop);
	//std::cout << "Read " << ncycread << " ByteSeq: MB/s " << (float) (ncycread * Demo::ByteSeqSize / time) / 1048576 << std::endl;
	cout << "Result: it took  " << time << " s" << endl;
	cout << "Result: rate: " << setprecision(10) << totbytes / 1000000 / time << " MB/s" << endl;
	cout << totbytes << endl;
	//exit(1);
}

double rate() {
	clock_gettime( CLOCK_MONOTONIC, &stop);
	double time = timediff(start, stop);
	//std::cout << "Read " << ncycread << " ByteSeq: MB/s " << (float) (ncycread * Demo::ByteSeqSize / time) / 1048576 << std::endl;
	//cout << "Result: it took  " << time << " s" << endl;
	return totbytes / (1024*1024) / time;
}

double ratepacketssent() {
	clock_gettime( CLOCK_MONOTONIC, &stop);
	double time = timediff(start, stop);
	//std::cout << "Read " << ncycread << " ByteSeq: MB/s " << (float) (ncycread * Demo::ByteSeqSize / time) / 1048576 << std::endl;
	//cout << "Result: it took  " << time << " s" << endl;
	return npacketssent / time;
}

void freeBuff (void *data, void *hint)
{
	return;
}


int main (int argc, char *argv [])
{
	const char *connect_to;
	int message_count;
	int message_size;
	void *context;
	void *sender;
	int rc;
	int i;
	zmq_msg_t msg;
	void *watch;
	unsigned long elapsed;
	
	cout << "Start EB Sim" << endl;
	// check arguments
	if(argc < 3)
	{
		std::cerr << "Error: wrong number of arguments. Usage:" << std::endl;
		std::cerr << argv[0] << " conf.xml file.raw [delay (usec)]" << std::endl;
		return EXIT_FAILURE;
	}
	
	double usecs = 0;
	if(argc == 3)
		usecs = std::atof(argv[3]);
	
	
	connect_to = "tcp://localhost:5555";
	//connect_to = "ipc:///tmp/feeds/0";
	message_size = 128000;
	message_count = 0;
	
	context = zmq_init (1);
	if (!context) {
		printf ("error in zmq_init: %s\n", zmq_strerror (errno));
		return -1;
	}
	
	sender = zmq_socket (context, ZMQ_PUSH);
	if (!sender) {
		printf ("error in zmq_socket: %s\n", zmq_strerror (errno));
		return -1;
	}
	
	//  Add your socket options here.
	//  For example ZMQ_RATE, ZMQ_RECOVERY_IVL and ZMQ_MCAST_LOOP for PGM.
	rc = zmq_connect (sender, connect_to);
	if (rc != 0) {
		printf ("error in zmq_connect: %s\n", zmq_strerror (errno));
		return -1;
	}
	
	//  Socket for control input
	/*
	void *controller = zmq_socket (context, ZMQ_SUB);
	zmq_connect (controller, "tcp://*:5500");
	zmq_setsockopt (controller, ZMQ_SUBSCRIBE, "", 0);
	*/
	
	cout << "start" << endl;
	// load the raw file.
	PacketLib::PacketBufferV* buff;
	try
	{
		buff = new PacketLib::PacketBufferV(argv[1], argv[2]);
		buff->load();
		std::cout << "Loaded buffer of " << buff->size() << " packets." << std::endl;
		
	} catch(PacketException* e) {
		cout << e->geterror() << endl;
	}
	
	npacketssent = 0;
	npacketssent_thread = 0;
	totbytes = 0;
	sumtotbytes = 0;
	sumnpacketssent = 0;
	
	clock_gettime( CLOCK_MONOTONIC, &start);
	size_t byteSent = 0;
	size_t buffsize = 128000;
	
	watch = zmq_stopwatch_start ();
	while (true) {
		
		
		// get a Packet
		PacketLib::ByteStreamPtr buffPtr = buff->getNext();
		
		// wait a little
		usleep(usecs);
	
		// send data to the RTAReceiver
		size_t buffsize = buffPtr->size();
		
		//copy message
		byte* stream = buffPtr->getStream();
	
		
		rc = zmq_msg_init_data (&msg, (void*)stream, buffsize, freeBuff, NULL);
		if (rc != 0) {
			printf ("error in zmq_msg_init_data: %s\n", zmq_strerror (errno));
			return -1;
		}
		
		/*
		rc = zmq_msg_init_size (&msg, buffsize);
		if (rc != 0) {
			printf ("error in zmq_msg_init_size: %s\n", zmq_strerror (errno));
			return -1;
		}
		*/
		
		totbytes += buffsize;
		sumtotbytes += buffsize;
		
		//cout << "send message " << buffsize << " bytes" << endl;
		rc = zmq_sendmsg (sender, &msg, 0);
		//cout << "message sent" << endl;
		if (rc < 0) {
			printf ("error in zmq_sendmsg: %s\n", zmq_strerror (errno));
			return -1;
		}
		rc = zmq_msg_close (&msg);
		if (rc != 0) {
			printf ("error in zmq_msg_close: %s\n", zmq_strerror (errno));
			return -1;
		}
		
		/*
		//controller
		zmq_pollitem_t items [] = {
			{ controller, 0, ZMQ_POLLIN, 0 }
		};
		zmq_poll (items, 1, 0);
		if (items [0].revents & ZMQ_POLLIN) {
			
			cout << "control message received" << endl;
		}
		sleep(1);
		*/
		
		npacketssent++;
		
		sumnpacketssent++;
		
		// byte sent used only to send the rate to the Monitor
		
		//mutex.lock();
		byteSent += buffsize;
		npacketssent_thread++;
		//mutex.unlock();
		
		if(npacketssent == 100000) {
			elapsed = zmq_stopwatch_stop (watch);
			if (elapsed == 0)
				elapsed = 1;
			
			cout << setprecision(10) << totbytes / (1024.*1024.) / (double) elapsed * 1000000 << " MiB/s" << endl;
			cout << setprecision(10) << npacketssent / (double) elapsed * 1000000 << " packets/s" << endl;
			cout << setprecision(10) << sumtotbytes / (1024*1024*1024) << " GiB " << endl;
			cout << setprecision(10) << sumnpacketssent << " packets" << endl;
			
			totbytes = 0;
			npacketssent = 0;
			clock_gettime( CLOCK_MONOTONIC, &start);
			watch = zmq_stopwatch_start ();
		}

	}
	
	rc = zmq_close (sender);
	if (rc != 0) {
		printf ("error in zmq_close: %s\n", zmq_strerror (errno));
		return -1;
	}
	
	rc = zmq_term (context);
	if (rc != 0) {
		printf ("error in zmq_term: %s\n", zmq_strerror (errno));
		return -1;
	}
	
	return 0;
}
