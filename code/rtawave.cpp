/***************************************************************************
 rtawave.cpp
 -------------------
 copyright            : (C) 2015 Andrea Zoli
 email                : zoli@iasfbo.inaf.it
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
#include <string>
#include <CTAMDArray.h>
#include <RTAWaveformExtractor.h>
#include <RTACleaning.h>
#include <RTADataProto.h>
#include <packet/ByteStream.h>
#include <packet/PacketStream.h>

using namespace std;
using namespace RTAAlgorithm;

int main (int argc, char *argv [])
{
	if(argc < 3)
	{
		std::cerr << "Error: wrong number of arguments. Usage:" << std::endl;
		std::cerr << argv[0] << "conf.xml port (ex. 6661, 6662 or 6663)" << std::endl;
		return EXIT_FAILURE;
	}


	PacketLib::PacketStream ps(argv[1]);
	PacketLib::Packet *p = ps.getPacketType("triggered_telescope1_30GEN");
	int npix_idx = p->getPacketSourceDataField()->getFieldIndex("Number of pixels");
	int nsamp_idx = p->getPacketSourceDataField()->getFieldIndex("Number of samples");
	int telID_idx = p->getPacketDataFieldHeader()->getFieldIndex("TelescopeID");
	int evtn_idx = p->getPacketDataFieldHeader()->getFieldIndex("eventNumber");
	int ntelsevt_idx = p->getPacketDataFieldHeader()->getFieldIndex("numberOfTriggeredTelescopes");

	CTAConfig::CTAMDArray array_conf;
	array_conf.loadConfig("AARPROD2", "PROD2_telconfig.fits.gz", "Aar.conf", "../share/ctaconfig/");

	RTAWaveformExtractor waveextractor(0, 0, 0, 6);
	RTACleaning cleaning(&array_conf, 0, 0, 10, 30);

	zmq::context_t context;
	zmq::socket_t sock(context, ZMQ_PULL);
	std::string address = std::string("tcp://*:") + argv[2];
	sock.bind(address.c_str());
	cout << "Waiting the receiver connection.." << endl;
	unsigned long nummessages;
	sock.recv(&nummessages, sizeof(nummessages), 0);
	cout << "Connected to receiver." << endl;

	cout << "Processing started!" << endl;

	unsigned long message_count = 0;
	unsigned long print_counter = 0;
	unsigned long totbytes = 0;
	unsigned long sumtotbytes = 0;

	void* watch = zmq_stopwatch_start ();

	zmq::message_t message;
	while(message_count < nummessages) {
		sock.recv(&message);

		size_t buffsize = message.size();
		if(buffsize==1 && *(char*)message.data() == EOF) {
			std::cout << "Shoutdown correctly!" << std::endl;
			break;
		}

		try {
		// Conversion Packet to RTAData_Camera. TODO a RTAAlgorithms class to do the conversion..
		PacketLib::ByteStreamPtr stream = PacketLib::ByteStreamPtr(new PacketLib::ByteStream((PacketLib::byte*)message.data(), message.size(), false));
		PacketLib::Packet *p = ps.getPacket(stream);
			
		PacketLib::ByteStreamPtr data = p->getData();
#ifdef ARCH_BIGENDIAN
		if(!data->isBigendian())
			data->swapWord();
#else
		if(data->isBigendian())
			data->swapWord();
#endif
		RTAData_Camera* cam = new RTAData_Camera;
		cam->nbytes = data->size();
		cam->data = new uint16_t[data->size()];
		memcpy(cam->data, data->getStream(), data->size());
		//cam->npix = p->getPacketSourceDataField()->getFieldValue(npix_idx);
		//cam->nsamp = p->getPacketSourceDataField()->getFieldValue(nsamp_idx);
		cam->telID = p->getPacketDataFieldHeader()->getFieldValue(telID_idx);
		cam->evtID = p->getPacketDataFieldHeader()->getFieldValue(evtn_idx);
		cam->ntelsEvt = p->getPacketDataFieldHeader()->getFieldValue(ntelsevt_idx);
		
		CTAConfig::CTAMDTelescopeType* teltype = array_conf.getTelescope(cam->telID)->getTelescopeType();
		int telTypeSim = teltype->getID();
		//std::cout << "telType: " << telTypeSim << std::endl;
		cam->npix = teltype->getCameraType()->getNpixels();
		cam->nsamp = teltype->getCameraType()->getPixel(0)->getPixelType()->getNSamples();

		// waveform extraction
		RTAData_CameraExtracted* integrated = (RTAData_CameraExtracted*) waveextractor.process(cam);

		// cleaning
		cleaning.process(integrated);
			
		delete integrated;

		message_count++;
		print_counter++;
		totbytes += buffsize;
		sumtotbytes += buffsize;

		if(print_counter == 1000) {
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
			

		delete cam;

			
		} catch (PacketLib::PacketException* e)
		{
			cout << e->geterror() << endl;
			
		}
	}
	return 0;
}