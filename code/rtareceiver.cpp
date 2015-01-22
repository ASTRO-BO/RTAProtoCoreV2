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

#include <zmq.h>
#include <zmq_utils.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <iostream>
#include <packet/PacketBufferV.h>
#include <packet/PacketLibDefinition.h>

//#define DEBUG 1

using namespace std;
using namespace PacketLib;

int main (int argc, char *argv [])
{
	const char *bind_to;
	int message_count;
	size_t message_size;
	void *context;
	void *s;
	int rc;
	//int i;
	zmq_msg_t msg;
	void *watch;
	unsigned long elapsed;
	unsigned long throughput;
	double megabits;
	unsigned long int totbytes = 0;

	if(argc < 2)
	{
		std::cerr << "Error: wrong number of arguments. Usage:" << std::endl;
		std::cerr << argv[0] << " conf.xml" << std::endl;
		return EXIT_FAILURE;
	}
	
	PacketStream* ps = NULL;
	try {
		ps = new PacketStream(argv[1]);
		Packet *p = ps->getPacketType("triggered_telescope1_30GEN");
		p->getPacketSourceDataField()->getFieldIndex("Number of pixels");
		p->getPacketSourceDataField()->getFieldIndex("Number of samples");
		p->getPacketDataFieldHeader()->getFieldIndex("TelescopeID");
	} catch (PacketException* e)
	{
		cout << "Error during extractWavePacket: ";
		cout << e->geterror() << endl;
	}
	

	
	bind_to = "tcp://*:5555";
	std::cout << "bind to " << bind_to << std::endl;
	//bind_to = "ipc:///tmp/feeds/0";
	
	message_count = 0;
	
	context = zmq_init (1);
	if (!context) {
		printf ("error in zmq_init: %s\n", zmq_strerror (errno));
		return -1;
	}
	
	s = zmq_socket (context, ZMQ_PULL);
	if (!s) {
		printf ("error in zmq_socket: %s\n", zmq_strerror (errno));
		return -1;
	}
	
	//  Add your socket options here.
	//  For example ZMQ_RATE, ZMQ_RECOVERY_IVL and ZMQ_MCAST_LOOP for PGM.
	
	rc = zmq_bind (s, bind_to);
	if (rc != 0) {
		printf ("error in zmq_bind: %s\n", zmq_strerror (errno));
		return -1;
	}
	
	rc = zmq_msg_init (&msg);
	if (rc != 0) {
		printf ("error in zmq_msg_init: %s\n", zmq_strerror (errno));
		return -1;
	}
	
	
	rc = zmq_recvmsg (s, &msg, 0);
	if (rc < 0) {
		printf ("error in zmq_recvmsg: %s\n", zmq_strerror (errno));
		return -1;
	}
	cout << "first message received" << endl;
	
	watch = zmq_stopwatch_start ();
	
	while(true) {
		//cout << "receive message" << endl;
		rc = zmq_recvmsg (s, &msg, 0);
		
		if (rc < 0) {
			printf ("error in zmq_recvmsg: %s\n", zmq_strerror (errno));
			return -1;
		}
		
		message_count++;
		message_size = zmq_msg_size (&msg);
		
		byte* stream = (byte*) zmq_msg_data (&msg);
		
		try {
			ByteStreamPtr rawPacket = ByteStreamPtr(new ByteStream(stream, message_size, ps->isBigEndian()));
			
			
			Packet *p = ps->getPacket(rawPacket);
			
			if(p->getPacketID() > 0) {
				//int npix = p->getPacketSourceDataField()->getFieldValue(npix_idx);
				//int nsamp = p->getPacketSourceDataField()->getFieldValue(nsamp_idx);
				//int telID = p->getPacketDataFieldHeader()->getFieldValue(telid_idx);
				
				ByteStreamPtr data = p->getData();
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
	#ifdef DEBUG
			std::cout << "npixels " << npix << std::endl;
			std::cout << "nsamples " << nsamp << std::endl;
			std::cout << "data size " << data->size() << std::endl;
			std::cout << "totbytes " << totbytes << std::endl;
	#endif
		 
			
		}
		catch (PacketException* e)
		{
				cout << "Error during extractWavePacket: ";
				cout << e->geterror() << endl;
		}
		
		//cout << "message received " << message_size << endl;
		if(message_count == 100000) {
			elapsed = zmq_stopwatch_stop (watch);
			if (elapsed == 0)
				elapsed = 1;
			
			throughput = (unsigned long)
			((double) message_count / (double) elapsed * 1000000);
			megabits = (double) (throughput * message_size * 8) / 1000000;
			
			printf ("message size: %d [B]\n", (int) message_size);
			printf ("message count: %d\n", (int) message_count);
			printf ("mean throughput: %d [msg/s]\n", (int) throughput);
			printf ("mean throughput: %.3f [Mb/s]\n", (double) megabits);
			watch = zmq_stopwatch_start ();
			message_count = 0;
		}
	}
	
	
	
	rc = zmq_msg_close (&msg);
	if (rc != 0) {
		printf ("error in zmq_msg_close: %s\n", zmq_strerror (errno));
		return -1;
	}
	
	
	rc = zmq_close (s);
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
