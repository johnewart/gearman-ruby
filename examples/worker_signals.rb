require 'rubygems'
#require 'gearman'
require '../lib/gearman'

Gearman::Util.debug = true

servers = ['localhost:4730', 'localhost:4731']
w = Gearman::Worker.new(servers)

# Add a handler for a "sleep" function that takes a single argument, the
# number of seconds to sleep before reporting success.
w.add_ability('sleep') do |data,job|
 seconds = data
 (1..seconds.to_i).each do |i|
   sleep 1
   Gearman::Util.logger.info i
   # Report our progress to the job server every second.
   job.report_status(i, seconds)
 end
 # Report success.
 true
end

# Trap signals while is working
%w(HUP USR1 ALRM TERM).each do |signal|
  trap(signal) do
    puts "Received signal #{signal} - setting worker_enabled to false. Worker status is [#{w.status}]"
    w.worker_enabled = false
    if w.status == :waiting
      trap(signal, "DEFAULT")
      Process.kill( signal, $$ )
    end
  end
end

loop { w.work or break }
