/***************************************************************************
 rtareceiver.cpp
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
#include <packet/PacketStream.h>
#include <CTAMDArray.h>

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

	CTAConfig::CTAMDArray array_conf;
	array_conf.loadConfig("AARPROD2", "PROD2_telconfig.fits.gz", "Aar.conf", "../share/ctaconfig/");
	
	PacketStream ps(argv[1]);

	zmq::context_t context;
	zmq::socket_t sock(context, ZMQ_PULL);
	sock.bind("tcp://*:5555");
	unsigned long nummessages;
	cout << "Waiting the ebsim connection.." << endl;
	sock.recv(&nummessages, sizeof(nummessages), 0);
	cout << "Connected, receiving " << nummessages << " messages.." << endl;

	zmq::context_t context1;
	zmq::socket_t sock1(context1, ZMQ_PUSH);
	sock1.connect("tcp://localhost:6661");
	cout << "Waiting the rtawave1 connection.." << endl;
	sock1.send(&nummessages, sizeof(nummessages), 0);
	cout << "Connected to rtawave1." << endl;

	zmq::context_t context2;
	zmq::socket_t sock2(context2, ZMQ_PUSH);
	sock2.connect("tcp://localhost:6662");
	cout << "Waiting the rtawave2 connection.." << endl;
	sock2.send(&nummessages, sizeof(nummessages), 0);
	cout << "Connected to rtawave2." << endl;

	zmq::context_t context3;
	zmq::socket_t sock3(context3, ZMQ_PUSH);
	sock3.connect("tcp://localhost:6663");
	cout << "Waiting the rtawave3 connection.." << endl;
	sock3.send(&nummessages, sizeof(nummessages), 0);
	cout << "Connected to rtawave3." << endl;

	cout << "Routing started!" << endl;

	unsigned long message_count = 0;
	unsigned long print_counter = 0;
	unsigned long totbytes = 0;
	unsigned long sumtotbytes = 0;

	void* watch = zmq_stopwatch_start ();

	zmq::message_t message;
	while(message_count < nummessages) {
		sock.recv(&message);
		
		size_t buffsize = message.size();

		/// decoding packetlib packet
		PacketLib::ByteStreamPtr stream = PacketLib::ByteStreamPtr(new PacketLib::ByteStream((PacketLib::byte*)message.data(), message.size(), false));
		PacketLib::Packet *packet = ps.getPacket(stream);

		if(packet && packet->getPacketID() > 0) {
			/// decoding packetlib packet
			PacketLib::ByteStreamPtr stream = PacketLib::ByteStreamPtr(new PacketLib::ByteStream((PacketLib::byte*)message.data(), message.size(), false));
			PacketLib::Packet *packet = ps.getPacket(stream);

			/// get telescope id
			PacketLib::DataFieldHeader* dfh = packet->getPacketDataFieldHeader();
			const unsigned int telescopeID = dfh->getFieldValue_16ui("TelescopeID");

/*			/// get the waveforms
			PacketLib::byte* wavebuff = packet->getData()->getStream();
			PacketLib::dword wavebuffsize = packet->getData()->size();*/

			/// get npixels and nsamples from ctaconfig using the telescopeID
			CTAConfig::CTAMDTelescopeType* teltype = array_conf.getTelescope(telescopeID)->getTelescopeType();
			int telTypeSim = teltype->getID();
			std::cout << "telType: " << telTypeSim << std::endl;
//			const unsigned int npixels = teltype->getCameraType()->getNpixels();
//			const unsigned int nsamples = teltype->getCameraType()->getPixel(0)->getPixelType()->getNSamples();

			// send to wave processors
			switch(telTypeSim) {
				case 138704810: { // large
					sock1.send(message);
					break;
				}
				case 10408418: { // medium
					sock2.send(message);
					break;
				}
				case 3709425: { // small
					sock3.send(message);
					break;
				}
				default: {
					std::cerr << "Warning: bad telescope type, skipping." << std::endl;
				}
			}
		}
		else
			std::cout << "some error decoding the packet" << std::endl;

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

	char eof = EOF;
	sock1.send(&eof, sizeof(eof), 0);
	sock2.send(&eof, sizeof(eof), 0);
	sock3.send(&eof, sizeof(eof), 0);

	sleep(3); // get an ack instead of guessing waiting time..

	return 0;
}
