require 'rubygems'
require 'gearman'
Gearman::Util.debug = true

servers = ['localhost:4730', 'localhost:4731']
  
client = Gearman::Client.new(servers, 'Test')
taskset = Gearman::TaskSet.new(client)

task = Gearman::Task.new('Sleep', {'seconds' => 20})
task.on_complete {|d| puts d }

taskset.add_task(task)
taskset.wait(100)
