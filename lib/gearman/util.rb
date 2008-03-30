#!/usr/bin/env ruby

require 'socket'
require 'time'

module Gearman

# = Util
#
# == Description
# Static helper methods and data used by other classes.
class Util
  # Map from Integer representations of commands used in the network
  # protocol to more-convenient symbols.
  COMMANDS = {
    1  => :can_do,          # W->J: FUNC
    23 => :can_do_timeout,  # W->J: FUNC[0]TIMEOUT
    2  => :cant_do,         # W->J: FUNC
    3  => :reset_abilities, # W->J: --
    22 => :set_client_id,   # W->J: [RANDOM_STRING_NO_WHITESPACE]
    4  => :pre_sleep,       # W->J: --

    6  => :noop,            # J->W: --
    7  => :submit_job,      # C->J: FUNC[0]UNIQ[0]ARGS
    21 => :submit_job_high, # C->J: FUNC[0]UNIQ[0]ARGS
    18 => :submit_job_bg,   # C->J: FUNC[0]UNIQ[0]ARGS

    8  => :job_created,     # J->C: HANDLE
    9  => :grab_job,        # W->J: --
    10 => :no_job,          # J->W: --
    11 => :job_assign,      # J->W: HANDLE[0]FUNC[0]ARG

    12 => :work_status,     # W->J/C: HANDLE[0]NUMERATOR[0]DENOMINATOR
    13 => :work_complete,   # W->J/C: HANDLE[0]RES
    14 => :work_fail,       # W->J/C: HANDLE

    15 => :get_status,      # C->J: HANDLE
    20 => :status_res,      # C->J: HANDLE[0]KNOWN[0]RUNNING[0]NUM[0]DENOM

    16 => :echo_req,        # ?->J: TEXT
    17 => :echo_res,        # J->?: TEXT

    19 => :error,           # J->?: ERRCODE[0]ERR_TEXT
  }

  # Map e.g. 'can_do' => 1
  NUMS = COMMANDS.invert

  # Default job server port.
  DEFAULT_PORT = 7003

  @@debug = false

  ##
  # Enable or disable debugging output (off by default).
  #
  # @param v  print debugging output
  def Util.debug=(v)
    @@debug = v
  end

  ##
  # Construct a request packet.
  #
  # @param type_name  command type's name (see COMMANDS)
  # @param arg        optional data to pack into the command
  # @return           packet (as a string)
  def Util.pack_request(type_name, arg='')
    type_num = NUMS[type_name.to_sym]
    raise InvalidArgsError, "Invalid type name '#{type_name}'" unless type_num
    arg = '' if not arg
    "\0REQ" + [type_num, arg.size].pack('NN') + arg
  end

  ##
  # Return a Task based on the passed-in arguments.
  #
  # @param args  either a single Task object or the arguments accepted by
  #              Task.new
  # @return      Task object
  def Util.get_task_from_args(*args)
    if args[0].class == Task
      return args[0]
    elsif args.size <= 3
      return Task.new(*args)
    else
      raise InvalidArgsError, 'Incorrect number of args to get_task_from_args'
    end
  end

  ##
  # Read from a socket, giving up if it doesn't finish quickly enough.
  # NetworkError is thrown if we don't read all the bytes in time.
  #
  # @param sock     Socket from which we read
  # @param len      number of bytes to read
  # @param timeout  maximum number of seconds we'll take; nil for no timeout
  # @return         full data that was read
  def Util.timed_recv(sock, len, timeout=nil)
    data = ''
    end_time = Time.now.to_f + timeout if timeout
    while data.size < len and (not timeout or Time.now.to_f < end_time) do
      IO::select([sock], nil, nil, timeout ? end_time - Time.now.to_f : nil) \
        or break
      data += sock.readpartial(len - data.size)
    end
    if data.size < len
      raise NetworkError, "Read #{data.size} byte(s) instead of #{len}"
    end
    data
  end

  ##
  # Read a response packet from a socket.
  #
  # @param sock     Socket connected to a job server
  # @param timeout  timeout in seconds, nil for no timeout
  # @return         array consisting of integer packet type and data
  def Util.read_response(sock, timeout=nil)
    end_time = Time.now.to_f + timeout if timeout
    head = timed_recv(sock, 12, timeout)
    magic, type, len = head.unpack('a4NN')
    raise ProtocolError, "Invalid magic '#{magic}'" unless magic == "\0RES"
    buf = len > 0 ?
      timed_recv(sock, len, timeout ? end_time - Time.now.to_f : nil) : ''
    type = COMMANDS[type]
    raise ProtocolError, "Invalid packet type #{type}" unless type
    [type, buf]
  end

  ##
  # Send a request packet over a socket.
  #
  # @param sock  Socket connected to a job server
  # @param req   request packet to send
  def Util.send_request(sock, req)
    len = sock.write(req)
    if len != req.size
      raise NetworkError, "Wrote #{len} instead of #{req.size}"
    end
  end

  ##
  # Add default ports to a job server or list of servers.
  #
  # @param servers  a server hostname or "host:port" or array of servers
  # @return         an array of "host:port" strings
  def Util.normalize_job_servers(servers)
    if servers.class == String or servers.class == Symbol
      servers = [ servers.to_s ]
    end
    servers.map {|s| s =~ /:/ ? s : "#{s}:#{DEFAULT_PORT}" }
  end

  ##
  # Convert job server info and a handle into a string.
  #
  # @param hostport  "host:port" of job server
  # @param handle    job server-returned handle for a task
  # @return          "host:port//handle"
  def Util.handle_to_str(hostport, handle)
    "#{hostport}//#{handle}"
  end

  ##
  # Reverse Util.handle_to_str.
  #
  # @param str  "host:port//handle"
  # @return     [hostport, handle]
  def Util.str_to_handle(str)
    str =~ %r{^([^:]+:\d+)//(.+)}
    return [$1, $3]
  end

  ##
  # Log a message if debugging is enabled.
  #
  # @param str  message to log
  def Util.log(str, force=false)
    puts "#{Time.now.strftime '%Y%m%d %H%M%S'} #{str}" if force or @@debug
  end

  ##
  # Log a message no matter what.
  #
  # @param str  message to log
  def Util.err(str)
    log(str, true)
  end
end

end
