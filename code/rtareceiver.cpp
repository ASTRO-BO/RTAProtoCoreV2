/***************************************************************************
 rtareceiver.cpp
 -------------------
 copyright            : (C) 2014 Andrea Bulgarelli
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
#include <packet/PacketLibDefinition.h>

using namespace std;
using namespace PacketLib;

int main (int argc, char *argv [])
{
	if(argc < 2)
	{
		std::cerr << "Error: wrong number of arguments. Usage:" << std::endl;
		std::cerr << argv[0] << " conf.xml" << std::endl;
		return EXIT_FAILURE;
	}
	
	PacketStream ps(argv[1]);

	zmq::context_t context;
	zmq::socket_t sock(context, ZMQ_PULL);
	sock.bind("tcp://*:5555");
	cout << "Waiting the ebsim connection.." << endl;

	unsigned long message_count = 0;
	unsigned long print_counter = 0;
	unsigned long totbytes = 0;
	unsigned long sumtotbytes = 0;

	unsigned long nummessages;
	sock.recv(&nummessages, sizeof(nummessages), 0);

	cout << "Receiving " << nummessages << " messages.." << endl;
	void* watch = zmq_stopwatch_start ();

	zmq::message_t message;
	while(message_count < nummessages) {
		sock.recv(&message);
		
		size_t buffsize = message.size();

		/// decoding packetlib packet
		PacketLib::ByteStreamPtr stream = PacketLib::ByteStreamPtr(new PacketLib::ByteStream((PacketLib::byte*)message.data(), message.size(), false));
		PacketLib::Packet *packet = ps.getPacket(stream);
			
		if(packet->getPacketID() > 0) {
			//int npix = p->getPacketSourceDataField()->getFieldValue(npix_idx);
			//int nsamp = p->getPacketSourceDataField()->getFieldValue(nsamp_idx);
			//int telID = p->getPacketDataFieldHeader()->getFieldValue(telid_idx);
				
			ByteStreamPtr data = packet->getData();
#ifdef ARCH_BIGENDIAN
			if(!data->isBigendian())
				data->swapWord();
#else
			if(data->isBigendian())
				data->swapWord();
#endif
			totbytes += data->size();
			//byte* rawdata = data->getStream();
		}

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
