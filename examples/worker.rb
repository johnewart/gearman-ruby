$LOAD_PATH.unshift("../lib")
require 'rubygems'
require 'logger'
require '../lib/gearman'
servers = ['localhost:4730']

w = Gearman::Worker.new(servers)
logger = Logger.new(STDOUT)

# Add a handler for a "sleep" function that takes a single argument, the
# number of seconds to sleep before reporting success.
w.add_ability("sleep") do |data,job|
 seconds = data.to_i
 logger.info "Sleeping for #{seconds} seconds"
 (1..seconds.to_i).each do |i|
   sleep 1
   # Report our progress to the job server every second.
   job.report_status(i, seconds)
 end
 # Report success.
 true
end

loop { w.work }
