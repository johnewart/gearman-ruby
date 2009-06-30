require 'rubygems'
#require 'gearman'
require '../lib/gearman'

Gearman::Util.debug = true

servers = ['localhost:4730', 'localhost:4731']
w = Gearman::Worker.new(servers)

ability_name_with_prefix = Gearman::Util.ability_name_with_prefix("test","sleep")

# Add a handler for a "sleep" function that takes a single argument, the
# number of seconds to sleep before reporting success.
w.add_ability(ability_name_with_prefix) do |data,job|
 seconds = data
 (1..seconds.to_i).each do |i|
   sleep 1
   print i
   # Report our progress to the job server every second.
   job.report_status(i, seconds)
 end
 # Report success.
 true
end
loop { w.work }
