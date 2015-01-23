/***************************************************************************
 rtaebsim.cpp
 -------------------
 copyright            : (C) 2014 Andrea Bulgarelli
                            2015 Andrea Zoli
 email                : bulgarelli@iasfbo.inaf.it
 ***************************************************************************/

/***************************************************************************
 *                                                                         *
 *   This program is free software for non commercial purpose              *
 *   modify it under the terms of the GNU General Public License.          *
 *   For commercial purpose see appropriate license terms                  *
 *                                                                         *
 ***************************************************************************/

#include "zmq.hpp"
#include <zmq_utils.h>
#include <iostream>
#include <iomanip>
#include <packet/PacketBufferV.h>

using namespace std;
using namespace PacketLib;

int main (int argc, char *argv [])
{
	cout << "Start EB Sim" << endl;

	// check arguments
	if(argc < 3)
	{
		std::cerr << "Error: wrong number of arguments. Usage:" << std::endl;
		std::cerr << argv[0] << " conf.xml file.raw [delay (usec)] [num messages]" << std::endl;
		return EXIT_FAILURE;
	}
	
	double usecs = 0;
	unsigned long nummessages = 300000;
	if(argc > 3)
		usecs = std::atof(argv[3]);
	if(argc > 4)
		nummessages = std::atof(argv[4]);
	
	zmq::context_t context;
	zmq::socket_t sock(context, ZMQ_PUSH);
	sock.connect("tcp://localhost:5555");
	cout << "Connected to rtareceiver." << endl;

	// load the raw file
	PacketLib::PacketBufferV buff(argv[1], argv[2]);
	buff.load();
	std::cout << "Loaded buffer of " << buff.size() << " packets." << std::endl;

	unsigned long message_count = 0;
	unsigned long print_counter = 0;
	unsigned long totbytes = 0;
	unsigned long sumtotbytes = 0;

	cout << "Sending.." << endl;

	sock.send(&nummessages, sizeof(nummessages), 0);
	
	void* watch = zmq_stopwatch_start();
	while (message_count < nummessages) {
		// get a Packet
		PacketLib::ByteStreamPtr buffPtr = buff.getNext();
		
		// wait a little
		usleep(usecs);
	
		// send data to the RTAReceiver
		size_t buffsize = buffPtr->size();
		
		//copy message
		byte* stream = buffPtr->getStream();

		
		sock.send(stream, buffsize, 0);
		
		message_count++;
		print_counter++;
		totbytes += buffsize;
		sumtotbytes += buffsize;
		
		if(print_counter == 100000) {
			unsigned long elapsed = zmq_stopwatch_stop(watch);
			if (elapsed == 0)
				elapsed = 1;
			
			cout << setprecision(10) << totbytes / (1024.*1024.) / (double) elapsed * 1000000 << " MiB/s" << endl;
			cout << setprecision(10) << print_counter / (double) elapsed * 1000000 << " packets/s" << endl;
			cout << setprecision(10) << sumtotbytes / (1024*1024*1024) << " GiB " << endl;
			cout << setprecision(10) << message_count << " packets" << endl;
			
			totbytes = 0;
			print_counter = 0;
			watch = zmq_stopwatch_start();
		}
	}

	return 0;
}
