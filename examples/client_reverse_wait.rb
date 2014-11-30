#!/usr/bin/env ruby
$:.unshift File.join(File.dirname(__FILE__), '..', "lib" )
require 'gearman'

# Client using Gearman SUBMIT_JOB_EPOCH (currently requires the gearmand branch lp:~jewart/gearmand/scheduled_jobs_support/)

t = nil
threadcounter = 0

client = Gearman::Client.new('localhost:4730')

  
myid = threadcounter 
threadcounter += 1
taskset = Gearman::TaskSet.new(client)
  
(1..100).each do |jid|
  data = rand(36**8).to_s(36)
  puts "#{jid} #{data}"
  task = Gearman::Task.new("reverse_string", data)
  task.on_complete {|d| puts d }
  client.do_task(task)
end


