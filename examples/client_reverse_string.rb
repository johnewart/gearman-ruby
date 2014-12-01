#!/usr/bin/env ruby
$:.unshift '../lib'
require 'gearman'

client = Gearman::Client.new('localhost:4730')

# Create 100 foreground jobs, one at a time
(1..100).each do |jid|
  data = rand(36**8).to_s(36)
  puts "#{jid} #{data}"
  task = Gearman::Task.new('reverse_string', data)
  task.on_complete {|d| puts d }
  client.do_task(task)
end


