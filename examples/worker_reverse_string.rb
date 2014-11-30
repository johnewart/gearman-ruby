$:.unshift File.join(File.dirname(__FILE__), '..', "lib" )
require 'gearman'

# String reverse worker 

servers = ['127.0.0.1:4730']

t = nil
jobnum = 0

w = Gearman::Worker.new(servers)
w.add_ability('reverse_string') do |data,job|
   result = data.reverse
   puts "Job: #{job.inspect} Data: #{data.inspect} Reverse: #{result} "
   puts "Completed job ##{jobnum}"
   jobnum += 1
   result
end

loop { w.work }


