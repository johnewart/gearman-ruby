#!/usr/bin/ruby

$: << '../lib'
require 'gearman'
require 'optparse'
require 'RMagick'

Gearman::Util.debug = true
servers = 'localhost:7003'

opts = OptionParser.new
opts.banner = "Usage: #{$0} [options]"
opts.on('-s SERVERS', '--servers',
  'Job servers, comma-separated host:port') { servers }
opts.parse!

worker = Gearman::Worker.new(servers.split(','), 'example')

worker.add_ability('scale_image') do |data,job|
  width, height, format, data = data.split("\0", 4)
  width = width.to_f
  height = height.to_f
  image = Magick::Image.from_blob(data)[0]
  orig_ratio = image.columns.to_f / image.rows
  new_ratio = width / height
  w = new_ratio < orig_ratio ? width : orig_ratio / new_ratio * width
  h = new_ratio > orig_ratio ? height : new_ratio / orig_ratio * height
  puts "Got #{image.inspect}; resizing to #{w}x#{h} #{format}"
  image.resize!(w, h)
  image.format = format
  image.to_blob
end

loop { worker.work }
