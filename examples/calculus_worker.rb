#!/usr/bin/env ruby
require 'rubygems'
require 'gearman'

#Gearman::Util.debug = true

# Additon worker
add_worker = Gearman::Worker.new('localhost')
add_worker.reconnect_sec = 2
add_worker.add_ability('addition') do |data,job|
  values = Marshal.load(data)
  puts "[addition_worker] Calculating #{values.inspect}..."
  sleep 5
  values.first + values.last
end

# Subtraction worker
sub_worker = Gearman::Worker.new('localhost')
sub_worker.reconnect_sec = 2
sub_worker.add_ability('subtraction') do |data,job|
  values = Marshal.load(data)
  puts "[subtraction_worker] Calculating #{values.inspect}..."
  sleep 5
  values.first - values.last
end

# Multiplication worker
mul_worker = Gearman::Worker.new('localhost')
mul_worker.reconnect_sec = 2
mul_worker.add_ability('multiplication') do |data,job|
  values = Marshal.load(data)
  puts "[multiplication_worker] Calculating #{values.inspect}..."
  sleep 5
  values.first * values.last
end

# Division worker
div_worker = Gearman::Worker.new('localhost')
div_worker.reconnect_sec = 2
div_worker.add_ability('division') do |data,job|
  values = Marshal.load(data)
  puts "[division_worker] Calculating #{data.inspect}..."
  sleep 5
  values.first / values.last
end

# Running the workers
loop do 
  puts '[worker] Starting workers...'
  add_worker.work
  sub_worker.work
  mul_worker.work
  div_worker.work
end
