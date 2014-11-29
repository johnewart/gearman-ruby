#!/usr/bin/env ruby

require 'logger'

module Gearman
  class << self
    attr_writer :logger
    def logger
      @logger ||= Logger.new(STDOUT)
    end
  end
end

require 'gearman/exceptions'
require 'gearman/logging'
require 'gearman/packet'
require 'gearman/connection'
require 'gearman/connection_pool'
require 'gearman/client'
require 'gearman/task'
require 'gearman/task_set'
require 'gearman/worker'