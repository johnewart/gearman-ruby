$LOAD_PATH.unshift("../lib")
require 'rubygems'
require 'gearman'

# Control logger
l = Logger.new($stdout)
l.level = Logger::DEBUG
Gearman.logger=l

servers = ['localhost:4730']
  
worker = Gearman::Worker.new(servers)

worker.add_ability('dedupe') do |data, job|
  puts "Should only see one of these come through!"
  true
end

loop { worker.work }
