#!/bin/bash  

# Start Gearmand
echo ' + Starting Gearmand'
gearmand --daemon --pidfile=/tmp/gearmand.pid

# Start the client and the worker(s)
echo ' + Starting calculus_worker.rb'
ruby calculus_worker.rb & 

sleep 3

echo ' + Starting calculus_client.rb'
ruby calculus_client.rb 

echo ' +++ Example finished +++ '

# Stop Gearmand
echo ' - Stopping Gearmand'
kill -9 `cat /tmp/gearmand.pid`

# Stop the workers
echo ' - Stopping calculus_worker.rb'
kill -9 `ps ax|grep calculus_worker|grep ruby|awk -F' ' '{print $1}'`

