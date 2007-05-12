#!/usr/bin/ruby

$: << '../lib'
require 'gearman'
require 'optparse'

Gearman::Util.debug = true
servers = 'localhost:7003'
format = 'PNG'
width, height = 100, 100

opts = OptionParser.new
opts.banner = "Usage: #{$0} [options] <input> <output>"
opts.on('-f FORMAT', '--format', 'Scaled image format') { format }
opts.on('-h HEIGHT', '--height', 'Scaled image height') { height }
opts.on('-s SERVERS', '--servers',
  'Servers, comma-separated host:port') { servers }
opts.on('-w WIDTH', '--width', 'Scaled image width') { width }
opts.parse!

if ARGV.size != 2
  $stderr.puts opts.banner
  exit 1
end

client = Gearman::Client.new(servers.split(','), 'example')
taskset = Gearman::TaskSet.new(client)
arg = [width, height, format, File.read(ARGV[0])].join("\0")
task = Gearman::Task.new('scale_image', arg)
task.on_complete {|d| File.new(ARGV[1],'w').write(d) }
taskset.add_task(task)
taskset.wait(10)
