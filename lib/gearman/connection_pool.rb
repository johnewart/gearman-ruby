require 'thread'

module Gearman
  class ConnectionPool
    include Logging

    DEFAULT_PORT = 4730

    def initialize(servers = [])
      @bad_servers          = []
      @coalesce_connections = {}
      @connection_handler   = nil
      @job_servers          = []
      @reconnect_seconds    = 10
      @server_counter       = 0   # Round-robin distribution of requests
      @servers_mutex        = Mutex.new

      add_servers(servers)
      start_reconnect_thread
    end

    def add_connection(connection)
      @servers_mutex.synchronize do
        if connection.is_healthy?
          activate_connection(connection)

          @connection_handler.call(connection) if @connection_handler
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
      @servers_mutex.synchronize do
        sockets = @job_servers.collect { |conn| conn.socket }
      end
      IO::select(sockets, nil, nil, timeout)
    end

    def with_all_connections(&block)
      @servers_mutex.synchronize do
        @job_servers.each do |connection|
          begin
            block.call(connection)
          rescue NetworkError => ex
            logger.debug "Error with #{connection}, marking as bad"
            remove_connection(connection)
          end
        end
      end
    end


    private

      def deactivate_connection(connection)
        @job_servers.reject! { |c| c == connection }
        @bad_servers << connection
      end

      def activate_connection(connection)
        @bad_servers.reject! { |c| c == connection }
        @job_servers << connection
      end

      def start_reconnect_thread
        Thread.new do
          loop do
            @servers_mutex.synchronize do
              # If there are any failed servers, try to reconnect to them.
              update_job_servers unless @bad_servers.empty?
            end
            sleep @reconnect_seconds
          end
        end.run
      end

      def update_job_servers
        logger.debug "Found #{@bad_servers.size} zombie connections, checking pulse."
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


  end
end
