require 'socket'

module Gearman

# = Client
#
# == Description
# A client for communicating with Gearman job servers.
class Client
  ##
  # Create a new client.
  #
  # @param job_servers  "host:port"; either a single server or an array
  def initialize(job_servers=nil)
    @job_servers = []  # "host:port"
    self.job_servers = job_servers if job_servers
    @sockets = {}  # "host:port" -> [sock1, sock2, ...]
    @socket_to_hostport = {}  # sock -> "host:port"
    @task_create_timeout_sec = 10
    @server_counter = -1
    @bad_servers = []
  end
  attr_reader :job_servers, :bad_servers
  attr_accessor :task_create_timeout_sec

  ##
  # Set the options
  #
  # @options options to pass to the servers  "exeptions"
  def option_request(opts)
    Util.logger.debug "GearmanRuby: Send options request with #{opts}"
    request = Util.pack_request("option_req", opts)
    sock= self.get_socket(self.get_job_server)
    Util.send_request(sock, request)
    response = Util.read_response(sock, 20)
    raise ProtocolError, response[1] if response[0]==:error
  end

  ##
  # Set the job servers to be used by this client.
  #
  # @param servers  "host:port"; either a single server or an array
  def job_servers=(servers)
    @job_servers = Util.normalize_job_servers(servers)
    self
  end

  ##
  # Get connection info about an arbitrary (currently random, but maybe
  # we'll do something smarter later) job server.
  #
  # @return  "host:port"
  def get_job_server
    if @job_servers.empty? && !@bad_servers.empty?
      Util.logger.debug "GearmanRuby: No more good job servers, trying bad ones: #{@bad_servers.inspect}."
      # Try to reconnect to the bad servers
      @bad_servers.each do |bad_server|
        Util.logger.debug "GearmanRuby: Trying server: #{bad_server.inspect}"
        begin
          request = Util.pack_request("echo_req", "ping")
          sock = self.get_socket(bad_server)
          Util.send_request(sock, request)
          response = Util.read_response(sock, 20)
          if response[0] == :echo_res
            @job_servers << bad_server
            @bad_servers.delete bad_server
          end
        rescue NetworkError
          Util.logger.debug "GearmanRuby: Error trying server: #{bad_server.inspect}"
        end
      end
    end

    Util.logger.debug "GearmanRuby: job servers: #{@job_servers.inspect}"
    raise NoJobServersError if @job_servers.empty?
    @server_counter += 1
    @job_servers[@server_counter % @job_servers.size]
  end

  def signal_bad_server(hostport)
    @job_servers = @job_servers.reject { |s| s == hostport }
    @bad_servers << hostport
  end

  ##
  # Get a socket for a job server.
  #
  # @param hostport  job server "host:port"
  # @return          a Socket
  def get_socket(hostport, num_retries=3)
    # If we already have an open socket to this host, return it.
    if @sockets[hostport]
      sock = @sockets[hostport].shift
      @sockets.delete(hostport) if @sockets[hostport].size == 0
      return sock
    end

    num_retries.times do |i|
      begin
        sock = TCPSocket.new(*hostport.split(':'))
      rescue Exception
        # Swallow error so we can retry -> num_retries times
      else
        # No error, stash socket mapping and return it
        @socket_to_hostport[sock] = hostport
        return sock
      end
    end
    raise NetworkError, "Unable to connect to job server #{hostport}"
  end

  ##
  # Relinquish a socket created by Client#get_socket.
  #
  # If we don't know about the socket, we just close it.
  #
  # @param sock  Socket
  def return_socket(sock)
    hostport = get_hostport_for_socket(sock)
    if not hostport
      inet, port, host, ip = sock.addr
      Util.logger.error "GearmanRuby: Got socket for #{ip}:#{port}, which we don't know about -- closing"
      sock.close
      return
    end
    (@sockets[hostport] ||= []) << sock
  end

  def close_socket(sock)
    sock.close
    @socket_to_hostport.delete(sock)
    nil
  end

  ##
  # Given a socket from Client#get_socket, return its host and port.
  #
  # @param sock  Socket
  # @return      "host:port", or nil if unregistered (which shouldn't happen)
  def get_hostport_for_socket(sock)
    @socket_to_hostport[sock]
  end

  ##
  # Perform a single task.
  #
  # @param args  either a Task or arguments for Task.new
  # @return      output of the task, or nil on failure
  def do_task(*args)
    task = Util::get_task_from_args(*args)

    result = nil
    failed = false
    task.on_complete {|v| result = v }
    task.on_fail { failed = true }

    taskset = TaskSet.new(self)
    if taskset.add_task(task)
      taskset.wait
    else
      raise JobQueueError, "Unable to enqueue job."
    end

    failed ? nil : result
  end

end

end