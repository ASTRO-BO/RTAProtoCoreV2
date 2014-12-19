/***************************************************************************
 rtacontroller.cpp
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

#include "zhelpers.h"
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <iostream>

using namespace std;

int main (int argc, char *argv [])
{
	const char *connect_to;
	char* message_string;
	int message_size;
	void *context;
	void *sender;
	int rc;
	int i;
	zmq_msg_t msg;
	
	
	if (argc != 2) {
		printf ("usage: controller <message string>\n");
		return 1;
	}
	
	connect_to = "tcp://*:5500";
	message_string = argv[1];
	
	context = zmq_init (1);
	if (!context) {
		printf ("error in zmq_init: %s\n", zmq_strerror (errno));
		return -1;
	}
	
	//  Socket for worker control
	void *controller = zmq_socket (context, ZMQ_PUB);
	zmq_bind (controller, connect_to);
	
	cout << "send message " << message_string << " to " << connect_to << endl;
	s_send (controller, message_string);
	zmq_close (controller);
	zmq_ctx_destroy (context);
	
	return 0;
}
