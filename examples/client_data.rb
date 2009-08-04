require 'rubygems'
#require 'gearman'
require '../lib/gearman'
Gearman::Util.debug = true

servers = ['localhost:4730']
  
client = Gearman::Client.new(servers)
taskset = Gearman::TaskSet.new(client)

task = Gearman::Task.new('chunked_transfer')
task.on_data {|d| puts d }
task.on_complete {|d| puts d }

taskset.add_task(task)
taskset.wait(100)
