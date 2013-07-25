#!/usr/bin/env ruby

require 'socket'
require 'gearman'

module Gearman

# = Server
#
# == Description
# A client for managing Gearman job servers.
class Server
  ##
  # Create a new client.
  #
  # @param job_servers  "host:port"; either a single server or an array
  # @param prefix       function name prefix (namespace)
  def initialize(hostport)
    @hostport = hostport  # "host:port"
  end
  attr_reader :hostport

  ##
  # Get a socket for a job server.
  #
  # @param hostport  job server "host:port"
  # @return          a Socket
  def socket(num_retries=3)
    return @socket if @socket
    num_retries.times do
      begin
        sock = TCPSocket.new(*hostport.split(':'))
      rescue Exception
      else
        return @socket = sock
      end
    end
    raise RuntimeError, "Unable to connect to job server #{hostport}"
  end

  ##
  # Sends a command to the server.
  #
  # @return a response string
  def send_command(name)
    response = ''
    socket.puts(name)
    while true do 
      if buf = socket.recv_nonblock(65536) rescue nil
        response << buf 
        return response if response =~ /^.$/
      end
    end
  end
  
  ##
  # Returns results of a 'status' command.
  #
  # @return a hash of abilities with queued, active and workers keys.
  def status
    status = {}
    if response = send_command('status')
      response.split("\n").each do |line|
        if line.match /^(.*)?\t(\d+)\t(\d+)\t(\d+)$/
          (status[$1] ||= {})[$2] = { :queue => $3, :active => $4, :workers => $5 }
        end
      end
    end
    status
  end
  
  ##
  # Returns results of a 'workers' command.
  #
  # @return an array of worker hashes, containing host, status and functions keys.
  def workers
    workers = []
    if response = send_command('workers')
      response.split("\n").each do |line|
        if line.match /^(\d+)\s([a-z0-9\:\.]+)\s([^\s]*)\s:\s([a-z_\s\t]+)$/
          func_parts = $4.split(' ')
          functions = []
          while !func_parts.empty?
            functions << func_parts.shift << '.' << func_parts.shift
          end
          workers << { :host => $2, :status => $3, :functions => functions }
        end
      end
    end
    workers
  end
end

end
