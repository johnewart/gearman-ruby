$:.unshift '../lib'
require 'gearman'

# String reverse worker 
servers = ['127.0.0.1:4730']
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


