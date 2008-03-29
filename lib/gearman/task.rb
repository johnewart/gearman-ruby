#!/usr/bin/env ruby

module Gearman

# = Task
#
# == Description
# A task submitted to a Gearman job server.
class Task
  ##
  # Create a new Task object.
  #
  # @param func  function name
  # @param arg   argument to the function
  # @param opts  hash of additional options
  def initialize(func, arg='', opts={})
    @func = func.to_s
    @arg = arg or ''  # TODO: use something more ref-like?
    %w{uniq on_complete on_fail on_retry on_status retry_count
       high_priority}.map {|s| s.to_sym }.each do |k|
      instance_variable_set "@#{k}", opts[k]
      opts.delete k
    end
    if opts.size > 0
      raise InvalidArgsError, 'Invalid task args: ' + opts.keys.sort.join(', ')
    end
    @retry_count ||= 0
    @successful = false
    @retries_done = 0
    @hash = nil
  end
  attr_accessor :uniq, :retry_count, :high_priority
  attr_reader :successful, :func, :arg

  ##
  # Internal method to reset this task's state so it can be run again.
  # Called by TaskSet#add_task.
  def reset_state
    @retries_done = 0
    @successful = false
    self
  end

  ##
  # Set a block of code to be executed when this task completes
  # successfully.  The returned data will be passed to the block.
  def on_complete(&f)
    @on_complete = f
  end

  ##
  # Set a block of code to be executed when this task fails.
  def on_fail(&f)
    @on_fail = f
  end

  ##
  # Set a block of code to be executed when this task is retried after
  # failing.  The number of retries that have been attempted (including the
  # current one) will be passed to the block.
  def on_retry(&f)
    @on_retry = f
  end

  ##
  # Set a block of code to be executed when we receive a status update for
  # this task.  The block will receive two arguments, a numerator and
  # denominator describing the task's status.
  def on_status(&f)
    @on_status = f
  end

  ##
  # Handle completion of the task.
  #
  # @param data  data returned from the server (doesn't include handle)
  def handle_completion(data)
    @successful = true
    @on_complete.call(data) if @on_complete
    self
  end

  ##
  # Record a failure and check whether we should be retried.
  #
  # @return  true if we should be resubmitted; false otherwise
  def handle_failure
    if @retries_done >= @retry_count
      @on_fail.call if @on_fail
      return false
    end
    @retries_done += 1
    @on_retry.call(@retries_done) if @on_retry
    true
  end

  ##
  # Handle a status update for the task.
  def handle_status(numerator, denominator)
    @on_status.call(numerator, denominator) if @on_status
    self
  end

  ##
  # Return a hash that we can use to execute identical tasks on the same
  # job server.
  #
  # @return  hashed value, based on @arg if @uniq is '-', on @uniq if it's
  #          set to something else, and just nil if @uniq is nil
  def get_uniq_hash
    return @hash if @hash
    merge_on = (@uniq and @uniq == '-') ? @arg : @uniq
    @hash = merge_on ? merge_on.hash.to_s : ''
  end

  ##
  # Construct a packet to submit this task to a job server.
  #
  # @param background  ??
  # @return            String representation of packet
  def get_submit_packet(prefix=nil, background=false)
    mode = 'submit_job' +
      (background ? '_bg' : @high_priority ? '_high' : '')
    func = (prefix ? prefix + "\t" : '') + @func
    Util::pack_request(mode, [func, get_uniq_hash, arg].join("\0"))
  end
end

end
