require 'rubygems'
require '../lib/gearman'
Gearman::Util.debug = true

servers = ['localhost:4730']

client = Gearman::Client.new(servers)
# make sure your gearmand supports propagation of worker exceptions!
# (standard impl. doesn't -- use XING fork from http://github.com/xing/gearman-server)
client.option_request("exceptions")

taskset = Gearman::TaskSet.new(client)

task = Gearman::Task.new('fail_with_exception', "void")
task.on_complete {|d| puts d }

tries = 0
task.on_exception do |message|
  tries += 1
  puts message
  tries < 2 # true should make gearmand reschedule the task
end

taskset.add_task(task)
taskset.wait(100)
