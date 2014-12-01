$:.push('../lib')
require 'gearman'
require 'logger'

logger = Logger.new(STDOUT)
logger.level = Logger::ERROR
Gearman.logger = logger

JOB_COUNT=100000

client = Gearman::Client.new('localhost:4730')

start_time = Time.now
(1..JOB_COUNT).each do |jid|
  data = rand(36**8).to_s(36)
  task = Gearman::BackgroundTask.new("reverse_string", data)
  client.do_task(task)
end
end_time = Time.now

diff = end_time - start_time
puts "Completed #{JOB_COUNT} jobs in #{diff} seconds, at #{JOB_COUNT.to_f / diff} JPS"
