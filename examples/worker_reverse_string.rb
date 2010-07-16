require 'rubygems'
require '../lib/gearman'

# String reverse worker 

servers = ['localhost:4730']

t = nil
jobnum = 0

(0..1).each do 
  t = Thread.new {
    w = Gearman::Worker.new(servers)
    w.add_ability('reverse_string') do |data,job|
      result = data.reverse
      puts "Job: #{job.inspect} Data: #{data.inspect} Reverse: #{result} "
      puts "Completed job ##{jobnum}"
      jobnum += 1
      result
    end
    loop { w.work }
  }
end

puts "Waiting for threads..."
t.join

