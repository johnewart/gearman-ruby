#!/usr/bin/env ruby
require 'rubygems'
require '../lib/gearman'
#Gearman::Util.debug = true

# Connect to the local server (at the default port 7003) 
client = Gearman::Client.new('localhost')
taskset = Gearman::TaskSet.new(client)

# Get something to echo
puts '[client] Type a string to reverse:'
input = gets.chomp

puts '[client] File to write to:'
outfile = gets.chomp

# Set scheduled time to 90 seconds from now
time = Time.now() + 30
puts "Time as seconds: #{time.to_i}" 
data = [input, outfile].join("\0")
task = Gearman::Task.new("reverse_to_file", data)
task.schedule(time)

# Sending the task to the server
puts "[client] Sending task: #{task.inspect}, to the 'reverse_to_file' worker"
taskset.add_task(task)
