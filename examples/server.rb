require 'rubygems'
# require 'gearman'
# require 'gearman/server'
require '../lib/gearman'
require '../lib/gearman/server'
require 'pp'

Gearman::Util.debug = true
w = Gearman::Server.new('localhost:4730')

loop {
  pp "Status: ", w.status
  pp "Workers: ", w.workers
  sleep 5
}