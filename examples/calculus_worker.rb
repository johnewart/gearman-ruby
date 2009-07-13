#!/usr/bin/env ruby
require 'rubygems'
require '../lib/gearman'

#Gearman::Util.debug = true

worker = Gearman::Worker.new('localhost')
worker.reconnect_sec = 2

# Additon ability
worker.add_ability('addition') do |data,job|
  values = Marshal.load(data)
  puts "[addition_worker] Calculating #{values.inspect}..."
  # sleep 5
  values.first + values.last
end

# Subtraction ability
worker.add_ability('subtraction') do |data,job|
  values = Marshal.load(data)
  puts "[subtraction_worker] Calculating #{values.inspect}..."
  # sleep 5
  values.first - values.last
end

# Multiplication worker
worker.add_ability('multiplication') do |data,job|
  values = Marshal.load(data)
  puts "[multiplication_worker] Calculating #{values.inspect}..."
  # sleep 5
  values.first * values.last
end

# Division worker
worker.add_ability('division') do |data,job|
  values = Marshal.load(data)
  puts "[division_worker] Calculating #{data.inspect}..."
  # sleep 5
  values.first / values.last
end

# Running the workers
loop do
  worker.work
end
