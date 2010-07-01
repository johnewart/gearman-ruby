require 'rubygems'
#require 'gearman'
require '../lib/gearman'

servers = ['localhost:4730']
w = Gearman::Worker.new(servers)

# Add a handler for a "sleep" function that takes a single argument, the
# number of seconds to sleep before reporting success.
w.add_ability('reverse_to_file') do |data,job|
  puts "Data: #{data.inspect}"
 word, file = data.split("\0")
 puts "Word: #{word}"
 puts "File: #{file}"
 # Report success.
 true
end
loop { w.work }
