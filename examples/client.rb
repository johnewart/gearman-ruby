$LOAD_PATH.unshift("../lib")
require 'rubygems'
require 'gearman'

# Control logger
l = Logger.new($stdout)
l.level = Logger::DEBUG
Gearman.logger=l

servers = ['localhost:4730']
  
client = Gearman::Client.new(servers)
taskset = Gearman::TaskSet.new(client)

task = Gearman::Task.new('sleep', 20)
task.on_status {|n,d| puts "Status: #{n}/#{d} iterations complete" }

# Add task to taskset
taskset.add_task(task)
# Submit taskset and wait forever for completion
taskset.wait_forever

puts "Ohai"
