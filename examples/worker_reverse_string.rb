require 'rubygems'
#require 'gearman'
require '../lib/gearman'

servers = ['localhost:4730']
w = Gearman::Worker.new(servers)

# Add a handler for a "sleep" function that takes a single argument, the
# number of seconds to sleep before reporting success.
w.add_ability('reverse_string') do |data,job|
 puts "Data: #{data.inspect} Reverse: #{data.reverse}"
 # Report success.
 true
end
loop { w.work }
