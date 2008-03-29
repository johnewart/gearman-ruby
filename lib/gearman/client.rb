#!/usr/bin/env ruby

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
  # @param prefix       function name prefix (namespace)
  def initialize(job_servers=nil, prefix=nil)
    @job_servers = []  # "host:port"
    self.job_servers = job_servers if job_servers
    @prefix = prefix
    @sockets = {}  # "host:port" -> [sock1, sock2, ...]
    @socket_to_hostport = {}  # sock -> "host:port"
    @test_hostport = nil  # make get_job_server return a given host for testing
    @task_create_timeout_sec = 10
  end
  attr_reader :job_servers
  attr_accessor :prefix, :test_hostport, :task_create_timeout_sec

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
    # Return a specific server if one's been set.
    @test_hostport or @job_servers[rand(@job_servers.size)]
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

    num_retries.times do
      begin
        sock = TCPSocket.new(*hostport.split(':'))
      rescue Exception
      else
        @socket_to_hostport[sock] = hostport
        return sock
      end
    end
    raise RuntimeError, "Unable to connect to job server #{hostport}"
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
      inet, port, host, ip = s.addr
      Util.err "Got socket for #{ip}:#{port}, which we don't " +
        "know about -- closing"
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
    taskset.add_task(task)
    taskset.wait

    failed ? nil : result
  end
end

end
