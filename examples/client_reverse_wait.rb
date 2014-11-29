#!/usr/bin/env ruby
require 'rubygems'
require '../lib/gearman'

# Client using Gearman SUBMIT_JOB_EPOCH (currently requires the gearmand branch lp:~jewart/gearmand/scheduled_jobs_support/)

t = nil
threadcounter = 0

client = Gearman::Client.new('localhost:4740')

  
myid = threadcounter 
threadcounter += 1
taskset = Gearman::TaskSet.new(client)
  
(1..100).each do |jid|
  data = rand(36**8).to_s(36)
  puts "#{jid} #{data}"
  task = Gearman::Task.new("reverse_string", data)
  task.on_complete {|d| puts d }
  taskset.add_task(task)
  taskset.wait(1000)
end


