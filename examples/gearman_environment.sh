#!/bin/bash  

# Start Gearmand
echo ' - Starting Gearmand'
gearmand --daemon --pidfile=/tmp/gearmand.pid

# Start the client and the worker(s)
echo ' - Starting calculus_worker.rb'
ruby calculus_worker.rb & 

echo ' - Starting calculus_client.rb'
ruby calculus_client.rb 

echo ' - Example finished'

# Stop Gearmand
echo ' - Stoping Gearmand'
kill -9 `cat /tmp/gearmand.pid`

# Stop the workers
#kill -9 `ps ax|grep ruby|grep 'calculus_worker.rb'|cut -f3 -d' '`
echo ' - Stop the worker manually'

