require 'rubygems'
require '../lib/gearman'
Gearman::Util.debug = true

servers = ['localhost:4730']

client = Gearman::Client.new(servers)
#try this out
client.option_request("exceptions")

taskset = Gearman::TaskSet.new(client)

task = Gearman::Task.new('fail_with_exception', 20)
task.on_complete {|d| puts d }
task.on_exception {|message| puts message; false}

taskset.add_task(task)
taskset.wait(100)
