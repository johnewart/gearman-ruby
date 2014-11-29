require 'socket'

module Gearman
  class Connection
    include Logging

    def initialize(hostname, port)
      @hostname = hostname
      @port = port
      @real_socket = nil
    end

    attr_reader :hostname, :port, :state, :socket

    ##
    # Check server health status by sending an ECHO request
    # Return true / false
    ##
    def is_healthy?
      if @real_socket == nil
        logger.debug "Performing health check for #{self}"
        begin
          request = Packet.pack_request("echo_req", "ping")
          response = send_request(request, 3)
          logger.debug "Health check response for #{self} is #{response.inspect}"
          raise ProtocolError unless response[0] == :echo_res and response[1] == "ping"
          return true
        rescue NetworkError
          logger.debug "NetworkError -- unhealthy"
          return false
        rescue ProtocolError
          logger.debug "ProtocolError -- unhealthy"
          return false
        end
      end
    end

    ##
    # @param num_retries  Number of times to retry
    # @return             This connection's Socket
    ##
    def socket(num_retries=3)
      # If we already have an open socket to this host, return it.
      return @real_socket if @real_socket
      num_retries.times do |i|
        begin
          logger.debug("Attempt ##{i} to connect to #{hostname}:#{port}")
          @real_socket = TCPSocket.new(hostname, port)
        rescue Exception => e
          logger.error("Unable to connect: #{e}")
          # Swallow error so we can retry -> num_retries times
        else
          return @real_socket
        end
      end

      raise_exception("Unable to connect to job server #{hostname}:#{port}")
    end

    def close_socket
      @real_socket.close if @real_socket
      @real_socket = nil
      true
    end

    def raise_exception(message)
      close_socket
      raise NetworkError, message
    end

    ##
    # Read from a socket, giving up if it doesn't finish quickly enough.
    # NetworkError is thrown if we don't read all the bytes in time.
    #
    # @param sock     Socket from which we read
    # @param len      number of bytes to read
    # @param timeout  maximum number of seconds we'll take; nil for no timeout
    # @return         full data that was read
    def timed_recv(sock, len, timeout=nil)
      data = ''
      start_time = Time.now.to_f
      end_time = Time.now.to_f + timeout if timeout
      while data.size < len and (not timeout or Time.now.to_f < end_time) do
        IO::select([sock], nil, nil, timeout ? end_time - Time.now.to_f : nil) \
        or break
        begin
          data += sock.readpartial(len - data.size)
        rescue
          close_socket
          raise NetworkError, "Unable to read data from socket."
        end
      end
      if data.size < len
        now = Time.now.to_f
        if now > end_time
          time_lapse = now - start_time
          raise SocketTimeoutError, "Took too long to read data: #{time_lapse} sec. to read on a  #{timeout} sec. timeout"
        else
          raise_exception("Read #{data.size} byte(s) instead of #{len}")
        end
      end
      data
    end

    ##
    # Read a response packet from a socket.
    #
    # @param sock     Socket connected to a job server
    # @param timeout  timeout in seconds, nil for no timeout
    # @return         array consisting of integer packet type and data
    def read_response(timeout=nil)
      end_time = Time.now.to_f + timeout if timeout
      head = timed_recv(socket, 12, timeout)
      magic, type, len = head.unpack('a4NN')
      raise ProtocolError, "Invalid magic '#{magic}'" unless magic == "\0RES"
      buf = len > 0 ?
          timed_recv(socket, len, timeout ? end_time - Time.now.to_f : nil) : ''
      type = Packet::COMMANDS[type]
      raise ProtocolError, "Invalid packet type #{type}" unless type
      [type, buf]
    end

    ##
    # Send a request packet over a socket that needs a response.
    #
    # @param sock  Socket connected to a job server
    # @param req   request packet to send
    # @result response from server
    def send_request(req, timeout = nil)
      send_update(req, timeout)
      return read_response(timeout)
    end

    def send_update(req, timeout = nil)
      len = with_safe_socket_op{ socket.write(req) }
      if len != req.size
        raise_exception("Wrote #{len} instead of #{req.size}")
      end
    end

    def with_safe_socket_op
      begin
        yield
      rescue Exception => ex
        raise_exception(ex.message)
      end
    end

    def to_host_port
      "#{hostname}:#{port}"
    end

    def to_s
      "#{hostname}:#{port} (connected: #{@real_socket != nil})"
    end

  end
end