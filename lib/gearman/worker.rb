#!/usr/bin/env ruby

require 'set'

require 'gearman/worker/callbacks'
require 'gearman/worker/ability'
require 'gearman/worker/job'

module Gearman

  class Worker
    include Logging
    include Callbacks

    ##
    # Create a new worker.
    #
    # @param job_servers  "host:port"; either a single server or an array
    # @param opts         hash of additional options
    def initialize(job_servers=nil, opts={})
      @abilities            = {}
      @client_id            = opts[:client_id] || generate_id
      @connection_pool      = ConnectionPool.new(job_servers)
      @network_timeout_sec  = opts[:network_timeout_sec] || 5
      @reconnect_sec        = opts[:reconnect_sec] || 30
      @status               = :preparing
      @worker_enabled       = true

      # Add callback for when connections occur -- register abilities and send client id
      @connection_pool.on_connection do |connection|
        connection.send_update(Packet.pack_request(:set_client_id, @client_id))
        @abilities.each do |func_name, ability|
          announce_ability(func_name, ability.timeout, connection)
        end
      end
    end

    attr_accessor :client_id, :reconnect_sec, :network_timeout_sec, :worker_enabled, :status

    ##
    # @return A random string of 30 characters from a-z
    def generate_id
      chars = ('a'..'z').to_a
      Array.new(30) { chars[rand(chars.size)] }.join
    end

    ##
    # Generate CAN_DO (or CAN_DO_TIMEOUT) packet and submit it
    def announce_ability(func_name, timeout, connection)
      cmd = timeout ? :can_do_timeout : :can_do
      arg = timeout ? "#{func_name}\0#{timeout.to_s}" : func_name
      connection.send_update(Packet.pack_request(cmd, arg))
      logger.debug "Announced ability #{func_name}"
    end

    ##
    # Add a new ability, announcing it to job servers.
    #
    # The passed-in block of code will be executed for jobs of this function
    # type.  It'll receive two arguments, the data supplied by the client and
    # a Job object.  If it returns nil or false, the server will be informed
    # that the job has failed; otherwise the return value of the block will
    # be passed back to the client in String form.
    #
    # @param func_name function name (without prefix)
    # @param timeout   the server will give up on us if we don't finish
    #                  a task in this many seconds
    # @param block     Block to associate with the function
    def add_ability(func_name, timeout=nil, &block)
      @abilities[func_name] = Ability.new(func_name, block, timeout)
      @connection_pool.with_all_connections do |connection|
        announce_ability(func_name, timeout, connection)
      end
    end

    ##
    # Callback for after an ability runs
    def after_ability(func, &block)
      abilities[func].after_complete(block)
    end

    ##
    # Let job servers know that we're no longer able to do something via CANT_DO
    #
    # @param func  function name
    def remove_ability(func)
      @abilities.delete(func)
      req = Packet.pack_request(:cant_do, func)
      @connection_pool.with_all_connections do  |connection|
        connection.send_update(req)
      end
    end

    ##
    # Handle a job_assign packet.
    #
    # @param data       data in the packet
    # @param connection Connection where the data originated
    def handle_job_assign(data, connection)
      handle, func, data = data.split("\0", 3)

      if not func
        logger.error "Ignoring JOB_ASSIGN with no function from #{connection}"
        return false
      end

      if not handle
        logger.error "Ignoring JOB_ASSIGN with no job handle from #{connection}"
        return false
      end

      logger.info "Got JOB_ASSIGN with handle #{handle} and #{data.size} byte(s) from #{connection}"

      ability = @abilities[func]

      if ability == nil
        logger.error "Ignoring JOB_ASSIGN for unsupported function #{func} with handle #{handle} from #{connection}"
        connection.send_update(Packet.pack_request(:work_fail, handle))
        return false
      end

      exception = nil
      begin
        ret = ability.run(data, Job.new(connection, handle))
      rescue Exception => e
        exception = e
        logger.debug "Exception: #{e}\n#{e.backtrace.join("\n")}\n"
      end

      packets = if ret && exception.nil?
              logger.debug "Sending WORK_COMPLETE for #{handle} with #{ret.to_s.size} byte(s) to #{connection}"
              run_work_complete_callback
              [Packet.pack_request(:work_complete, "#{handle}\0#{ret.to_s}")]
            elsif exception.nil?
              logger.debug "Sending WORK_FAIL for #{handle} to #{connection}"
              run_work_fail_callback
              [Packet.pack_request(:work_fail, handle)]
            elsif exception
              logger.debug "Sending WORK_EXCEPTION for #{handle} to #{connection}"
              run_work_exception_callback
              [Packet.pack_request(:work_exception, "#{handle}\0#{exception.message}")]
            end

      packets.each do |packet|
        connection.send_update(packet)
      end

      true
    end


    ##
    # Handle a message for the worker
    #
    # @param type       Packet type (NO_JOB, JOB_ASSIGN, NO_OP)
    # @param data       Opaque data being passed with the message
    # @param connection The Connection object where the message originates
    # @return
    def handle_work_message(type, data, connection)
      case type
        when :no_job
          logger.info "Got NO_JOB from #{connection}"
          run_no_job_callback
        when :job_assign
          @status = :working
          run_job_assign_callback
          return worker_enabled if handle_job_assign(data, connection)
        when :no_op
          # We'll have to read again
          logger.debug "Received NOOP while polling. Ignoring NOOP"
        else
          logger.error "Got unhandled #{type.to_s} from #{connection}"
      end
    end

    ##
    # Do a single job and return.
    def work
      grab_job_req = Packet.pack_request(:grab_job)
      type, data = nil

      loop do
        @status = :preparing
        @connection_pool.with_all_connections do |connection|
          logger.debug "Sending GRAB_JOB to #{connection}"
          run_grab_job_callback

          begin
            type, data = connection.send_request(grab_job_req, @network_timeout_sec)
            handle_work_message(type, data, connection)
          end while type == :no_op
        end

        logger.info "Sending PRE_SLEEP and going to sleep for #{@reconnect_sec} second(s)"
        @connection_pool.with_all_connections do |connection|
            connection.send_update(Packet.pack_request(:pre_sleep))
        end

        return false unless worker_enabled
        @status = :waiting

        time_asleep = Time.now

        while (@status == :waiting)
          sleep(time_asleep)
        end

      end
    end

    ##
    # Sleep and poll until timeout occurs or a NO_OP packet is received
    # @param time_fell_asleep The time that we fell asleep (Time object)
    def sleep(time_fell_asleep)
      # Use IO::select to wait for available connection data
      @connection_pool.poll_connections(@network_timeout_sec)

      # If 30 seconds have passed, then wakeup
      time_asleep = Time.now - time_fell_asleep
      @status = :wakeup if time_asleep > 30

      if (@status == :waiting)
        @connection_pool.with_all_connections do |connection|
          begin
            type, data = connection.read_response(@network_timeout_sec)

            # Wake up if we receive a NOOP packet
            if (type == :noop)
              logger.debug "Received NOOP while sleeping... waking up!"
              @status = :wakeup
            else
              logger.warn "Received something other than a NOOP packet while sleeping: #{type.to_s}"
            end
          rescue SocketTimeoutError
            # This is okay here.
          end
        end
      end
    end
  end

end
