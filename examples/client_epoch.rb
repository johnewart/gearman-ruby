#!/usr/bin/env ruby
require 'rubygems'
require '../lib/gearman'
#Gearman::Util.debug = true

# Connect to the local server (at the default port 7003) 
client = Gearman::Client.new('localhost')
taskset = Gearman::TaskSet.new(client)

data = rand(36**8).to_s(36)
# Set scheduled time to some time in the future
time = Time.now() + 30
puts "Time as seconds: #{time.to_i}" 
task = Gearman::Task.new("reverse_string", data)
task.schedule(time)

# Sending the task to the server
puts "[client] Sending task: #{task.inspect}, to the 'reverse_string' worker"
taskset.add_task(task)
