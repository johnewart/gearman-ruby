require 'rubygems'
require '../lib/gearman'
l = Logger.new($stdout)
l.level = Logger::DEBUG
Gearman::Util.logger=l

# String reverse worker 

servers = ['127.0.0.1:4730']

client = Gearman::Client.new(servers)
taskset = Gearman::TaskSet.new(client)
t = nil
jobnum = 0

w = Gearman::Worker.new(servers)
w.add_ability('reverse_string') do |data,job|
   result = data.reverse
   puts "Job: #{job.inspect} Data: #{data.inspect} Reverse: #{result} "
   puts "Completed job ##{jobnum}"
   data = rand(36**8).to_s(36)
   task = Gearman::BackgroundTask.new("background_job", data)
   taskset.add_task(task)
   jobnum += 1
   result
end

loop { w.work }


