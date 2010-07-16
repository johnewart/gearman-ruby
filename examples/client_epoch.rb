#!/usr/bin/env ruby

# Client using Gearman SUBMIT_JOB_EPOCH (currently requires the gearmand branch lp:~jewart/gearmand/scheduled_jobs_support/)

require 'rubygems'
require '../lib/gearman'

(1..100).each do 
   # Connect to the local server (at the default port 4730) 
   client = Gearman::Client.new('localhost')
   taskset = Gearman::TaskSet.new(client)

   data = rand(36**8).to_s(36)
   # Set scheduled time to some time in the future
   time = Time.now() + rand(10)
   puts "Time as seconds: #{time.to_i}" 
   task = Gearman::Task.new("reverse_string", data)
   task.schedule(time)

   # Sending the task to the server
   puts "[client] Sending task: #{task.inspect}, to the 'reverse_string' worker"
   taskset.add_task(task)
end
