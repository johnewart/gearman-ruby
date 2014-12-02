require 'thread'

module Gearman
  class ConnectionPool
    include Logging

    DEFAULT_PORT = 4730
    TIME_BETWEEN_CHECKS = 60 #seconds
    SLEEP_TIME = 30 #seconds

    def initialize(servers = [])
      @bad_servers          = []
      @coalesce_connections = {}
      @connection_handler   = nil
      @job_servers          = []
      @reconnect_seconds    = 10
      @server_counter       = 0   # Round-robin distribution of requests
      @servers_mutex        = Mutex.new
      @last_check_time      = Time.now

      add_servers(servers)
    end

    def add_connection(connection)
      @servers_mutex.synchronize do
        if connection.is_healthy?
          activate_connection(connection)
        else
          deactivate_connection(connection)
        end
      end
    end

    def add_host_port(host_port)
      host, port = host_port.split(":")
      connection = Connection.new(host, port.to_i)
      add_connection(connection)
    end

    def add_servers(servers)
      if servers.class == String or servers.class == Symbol
        servers = [ servers.to_s ]
      end

      servers = servers.map {|s| s =~ /:/ ? s : "#{s}:#{DEFAULT_PORT}" }

      servers.each do |host_port|
        add_host_port(host_port)
      end
    end

    def get_connection(coalesce_key = nil)
      @servers_mutex.synchronize do
        logger.debug "Available job servers: #{@job_servers.inspect}"
        raise NoJobServersError if @job_servers.empty?
        @server_counter += 1
        @job_servers[@server_counter % @job_servers.size]
      end
    end

    def on_connection(&block)
      @connection_handler = block
    end

    def poll_connections(timeout = nil)
      update_job_servers
      available_sockets = []
      @servers_mutex.synchronize do
        available_sockets.concat @job_servers.collect { |conn| conn.socket }
      end
      if available_sockets.size > 0
      	logger.debug "Polling on #{available_sockets.size} available server(s) with a #{timeout} second timeout"
      	IO::select(available_sockets, nil, nil, timeout)
      end
    end

    def with_all_connections(&block)
      update_job_servers
      @servers_mutex.synchronize do
        @job_servers.each do |connection|
          begin
            block.call(connection)
          rescue NetworkError => ex
            logger.debug "Error with #{connection}, marking as bad"
            deactivate_connection(connection)
          end
        end
      end
    end


    private

      def time_to_check_connections
        (Time.now - @last_check_time) >= TIME_BETWEEN_CHECKS
      end

      def deactivate_connection(connection)
        @job_servers.reject! { |c| c == connection }
        @bad_servers << connection
      end

      def activate_connection(connection)
        @bad_servers.reject! { |c| c == connection }
        @job_servers << connection
        @connection_handler.call(connection) if @connection_handler
      end

      ##
      # Check for bad servers and update the available pools
      #
      def update_job_servers
        # Check if it's been > TIME_BETWEEN_CHECKS or we have no good servers
        return unless time_to_check_connections || @job_servers.empty?

        logger.debug "Found #{@bad_servers.size} zombie connections, checking pulse."
        @servers_mutex.synchronize do
          @bad_servers.each do |connection|
            begin
              message = "Testing server #{connection}..."
              if connection.is_healthy?
                logger.debug "#{message} Connection is healthy, putting back into service"
                activate_connection(connection)
              else
                logger.debug "#{message} Still down."
              end
            end
          end
        end

        # Sleep for a few to allow a chance for the world to become sane
        if @job_servers.empty?
          logger.warn "No job servers available, sleeping for #{SLEEP_TIME} seconds"
          sleep(SLEEP_TIME)
        end

        @last_check_time = Time.now
      end


  end
end
