import zmq
import time
import sys

port = "5556"
#ip = "*"
ip="zitpcx19282.desy.de"

context = zmq.Context()
socket = context.socket(zmq.PUSH)
socket.connect("tcp://" + ip + ":%s" % port)

while True:
    message = ["World"]
    print "Send: ", message
    res = socket.send_multipart(message, copy=False, track=True)
    if res.done:
        print "res: done"
    else:
        print "res: waiting"
        res.wait()
        print "res: waiting..."
    print "sleeping..."
    time.sleep (1)
    print "sleeping...done"
