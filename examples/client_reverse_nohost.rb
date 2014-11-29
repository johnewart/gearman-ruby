#!/usr/bin/env ruby
require 'rubygems'
require '../lib/gearman'
l = Logger.new($stdout)
l.level = Logger::DEBUG
Gearman::Util.logger=l

# Client using Gearman SUBMIT_JOB_EPOCH (currently requires the gearmand branch lp:~jewart/gearmand/scheduled_jobs_support/)

t = nil
threadcounter = 0

client = Gearman::Client.new('192.168.1.1:4730')

  
myid = threadcounter 
threadcounter += 1
taskset = Gearman::TaskSet.new(client)
  
(1..1000).each do |jid|
  data = rand(36**8).to_s(36)
  result = data.reverse

  task = Gearman::BackgroundTask.new("reverse_string", data)
  puts "#{jid} #{data}"
        
  #time = Time.now() + rand(120) + 10
  #task.schedule(time)
  taskset.add_task(task)
end
