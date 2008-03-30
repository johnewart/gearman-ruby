#!/usr/bin/env ruby
#
# = Name
# Gearman
#
# == Description
# This file provides a Ruby interface for communicating with the Gearman
# distributed job system.
#
# "Gearman is a system to farm out work to other machines, dispatching
# function calls to machines that are better suited to do work, to do work
# in parallel, to load balance lots of function calls, or to call functions
# between languages."  -- http://www.danga.com/gearman/
#
# == Version
# 0.0.1
#
# == Author
# Daniel Erat <dan-ruby@erat.org>
#
# == License
# This program is free software; you can redistribute it and/or modify it
# under the terms of either:
#
# a) the GNU General Public License as published by the Free Software
#    Foundation; either version 1, or (at your option) any later version,
#    or
#
# b) the "Artistic License" which comes with Perl.

# = Gearman
#
# == Usage
#  require 'gearman'
#
#  # Create a new client and tell it about two job servers.
#  c = Gearman::Client.new
#  c.job_servers = ['127.0.0.1:7003', '127.0.0.1:7004']
#
#  # Create two tasks, using an "add" function to sum two numbers.
#  t1 = Gearman::Task.new('add', '5 + 2')
#  t2 = Gearman::Task.new('add', '1 + 3')
#
#  # Make the tasks print the data they get back from the server.
#  t1.on_complete {|d| puts "t1 got #{d}" }
#  t2.on_complete {|d| puts "t2 got #{d}" }
#
#  # Create a taskset, add the two tasks to it, and wait until they finish.
#  ts = Gearman::TaskSet.new(c)
#  ts.add_task(t1)
#  ts.add_task(t2)
#  ts.wait
#
# Or, a more simple example:
#
#  c = Gearman::Client.new('127.0.0.1')
#  puts c.do_task('add', '2 + 2')
#
module Gearman

require 'gearman/client'
require 'gearman/task'
require 'gearman/taskset'
require 'gearman/util'
require 'gearman/worker'

class InvalidArgsError < Exception
end

class ProtocolError < Exception
end

class NetworkError < Exception
end

end
