require 'time'

module Gearman
  class Client
    include Logging

    attr_accessor :task_create_timeout_sec

    ##
    # Create a new client.
    #
    # @param job_servers  "host:port"; either a single server or an array
    def initialize(job_servers=nil)
      @coalesce_connections = {}  # Unique ID => Connection
      @connection_pool = ConnectionPool.new(job_servers)
      @current_task = nil
      @task_create_timeout_sec = 10
    end

    ##
    # Set the options
    #
    # @options options to pass to the servers  i.e "exceptions"
    def set_options(opts)
      @connection_pool.with_all_connections do |conn|
        logger.debug "Send options request with #{opts}"
        request = Packet.pack_request("option_req", opts)
        response = conn.send_request(request)
        raise ProtocolError, response[1] if response[0]==:error
      end
    end

    ##
    # Perform a single task.
    #
    # @param args  A Task to complete
    # @return      output of the task, or nil on failure
    def do_task(task)

      result = nil
      failed = false

      task.on_complete {|v| result = v }
      task.on_fail { failed = true }

      task_set = TaskSet.new(self)
      if task_set.add_task(task)
        task_set.wait_forever
      else
        raise JobQueueError, "Unable to enqueue job."
      end

      failed ? nil : result
    end

    def submit_job(task, reset_state = false, timeout = nil)
      task.reset_state if reset_state
      req = task.get_submit_packet()
      req_timeout = timeout || task_create_timeout_sec
      # Target the same job manager when submitting jobs
      # with the same unique id so that we can coalesce
      coalesce_key = task.get_uniq_hash

      end_time = if timeout
                   Time.now.to_f + timeout
                 else
                   nil
                 end

      begin

        connection = @connection_pool.get_connection(coalesce_key)
        logger.debug "Using #{connection} to submit job"

        type, data = connection.send_request(req, timeout)
        logger.debug "Got #{type.to_s} from #{connection}"

        if type == :job_created

          task.handle_created(data)

          if(!task.background)
            begin
              remaining = if end_time
                            (t = end_time - Time.now.to_f) > 0 ? t : 0
                          else
                            nil
                          end
              type, data = connection.read_response(remaining)
              handle_response(task, type, data)
            end while [:work_status, :work_data, :work_warning].include? type
          end

        else
          # This shouldn't happen
          message = "Received #{type.to_s} when we were expecting JOB_CREATED"
          logger.error message
          raise ProtocolError, message
        end
      rescue NetworkError
        message = "Network error on read from #{connection.to_host_port} while adding job, marking server bad"
        logger.error message
        raise NetworkError, message
      rescue NoJobServersError
        logger.error "No servers available."
        raise NoJobServersError
      end

      true
    end

    def handle_response(task, type, data)
      case type
        when :work_complete
          handle, message = data.split("\0", 2)
          logger.debug("Received WORK_COMPLETE for #{handle}")
          task.handle_completion(message)
        when :work_exception
          handle, exception = data.split("\0", 2)
          logger.debug("Received WORK_EXCEPTION for #{handle}")
          task.handle_exception(exception)
        when :work_fail
          logger.debug("Received WORK_FAIL for #{handle}")
          requeue = task.handle_failure
          add_task(task) if requeue
        when :work_status
          handle, numerator, denominator = data.split("\0", 3)
          logger.debug("Received WORK_STATUS for #{handle}: #{numerator} / #{denominator}")
          task.handle_status(numerator, denominator)
        when :work_warning
          handle, message = data.split("\0", 2)
          logger.warn "Got WORK_WARNING for #{handle}: '#{message}'"
          task.handle_warning(message)
        when :work_data
          handle, work_data = data.split("\0", 2)
          logger.debug "Got WORK_DATA for #{handle} with #{work_data ? work_data.size : '0'} byte(s) of data"
          task.handle_data(work_data)
        else
          # Not good.
          message = "Got #{type.to_s} from #{connection} but it was not an expected type."
          logger.error message
          raise ProtocolError, message
      end
    end

  end




end
