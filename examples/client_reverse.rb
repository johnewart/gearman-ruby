#!/usr/bin/env ruby
require 'rubygems'
require '../lib/gearman'

# Client using Gearman SUBMIT_JOB_EPOCH (currently requires the gearmand branch lp:~jewart/gearmand/scheduled_jobs_support/)

t = nil
threadcounter = 0

client = Gearman::Client.new('localhost')

  
myid = threadcounter 
threadcounter += 1
taskset = Gearman::TaskSet.new(client)
  
(1..10000).each do |jid|
  data = rand(36**8).to_s(36)
  result = data.reverse

  task = Gearman::Task.new("reverse_string", data)
  puts "#{jid} #{data}"
        
  time = Time.now() + rand(120) + 10
  task.schedule(time)
  taskset.add_task(task)
end
