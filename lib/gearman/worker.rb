#!/usr/bin/env ruby

require 'set'
require 'socket'
require 'thread'

module Gearman

# = Worker
#
# == Description
# A worker that can connect to a Gearman server and perform tasks.
#
# == Usage
#  require 'gearman'
#
#  w = Gearman::Worker.new('127.0.0.1')
#
#  # Add a handler for a "sleep" function that takes a single argument, the
#  # number of seconds to sleep before reporting success.
#  w.add_ability('sleep') do |data,job|
#    seconds = data
#    (1..seconds.to_i).each do |i|
#      sleep 1
#      # Report our progress to the job server every second.
#      job.report_status(i, seconds)
#    end
#    # Report success.
#    true
#  end
#  loop { w.work }
class Worker
  # = Ability
  #
  # == Description
  # Information about an ability that we possess.
  class Ability
    ##
    # Create a new ability.
    #
    # @param block    code to run
    # @param timeout  server gives up on us after this many seconds
    def initialize(block, timeout=nil)
      @block = block
      @timeout = timeout
    end
    attr_reader :timeout

    ##
    # Run the block of code.
    #
    # @param data  data passed to us by a client
    # @param job   interface to report job information to the server
    def run(data, job)
      @block.call(data, job)
    end
  
  end

  # = Job
  #
  # == Description
  # Interface to allow a worker to report information to a job server.
  class Job
    ##
    # Create a new Job.
    #
    # @param sock    Socket connected to job server
    # @param handle  job server-supplied job handle
    attr_reader :handle
    def initialize(sock, handle)
      @socket = sock
      @handle = handle
    end

    ##
    # Report our status to the job server.
    def report_status(numerator, denominator)
      req = Util.pack_request(
        :work_status, "#{@handle}\0#{numerator}\0#{denominator}")
      Util.send_request(@socket, req)
      self
    end

    ##
    # Send data before job completes
    def send_data(data)
      req = Util.pack_request(:work_data, "#{@handle}\0#{data}")
      Util.send_request(@socket, req)
      self
    end

    ##
    # Send a warning explicitly
    def report_warning(warning)
      req = Util.pack_request(:work_warning, "#{@handle}\0#{warning}")
      Util.send_request(@socket, req)
      self
    end
  end

  ##
  # Create a new worker.
  #
  # @param job_servers  "host:port"; either a single server or an array
  # @param opts         hash of additional options
  def initialize(job_servers=nil, opts={})
    chars = ('a'..'z').to_a
    @client_id = Array.new(30) { chars[rand(chars.size)] }.join
    @sockets = {}  # "host:port" -> Socket
    @abilities = {}  # "funcname" -> Ability
    @after_abilities = {} # "funcname" -> Ability
    @bad_servers = []  # "host:port"
    @servers_mutex = Mutex.new
    %w{client_id reconnect_sec
       network_timeout_sec}.map {|s| s.to_sym }.each do |k|
      instance_variable_set "@#{k}", opts[k]
      opts.delete k
    end
    if opts.size > 0
      raise InvalidArgsError,
        'Invalid worker args: ' + opts.keys.sort.join(', ')
    end
    @reconnect_sec = 30 if not @reconnect_sec
    @network_timeout_sec = 5 if not @network_timeout_sec
    @worker_enabled = true
    @status = :preparing
    self.job_servers = job_servers if job_servers
    start_reconnect_thread
  end
  attr_accessor :client_id, :reconnect_sec, :network_timeout_sec, :bad_servers, :worker_enabled, :status

  # Start a thread to repeatedly attempt to connect to down job servers.
  def start_reconnect_thread
    Thread.new do
      loop do
        @servers_mutex.synchronize do
          # If there are any failed servers, try to reconnect to them.
          if not @bad_servers.empty?
            update_job_servers(@sockets.keys + @bad_servers)
          end
        end
        sleep @reconnect_sec
      end
    end.run
  end

  def job_servers
    servers = nil
    @servers_mutex.synchronize do
      servers = @sockets.keys + @bad_servers
    end
    servers
  end

  ##
  # Connect to job servers to be used by this worker.
  #
  # @param servers  "host:port"; either a single server or an array
  def job_servers=(servers)
    @servers_mutex.synchronize do
      update_job_servers(servers)
    end
  end

  # Internal function to actually connect to servers.
  # Caller must acquire @servers_mutex before calling us.
  #
  # @param servers  "host:port"; either a single server or an array
  def update_job_servers(servers)
    @bad_servers = []
    servers = Set.new(Util.normalize_job_servers(servers))
    # Disconnect from servers that we no longer care about.
    @sockets.each do |server,sock|
      if not servers.include? server
        Util.logger.info "GearmanRuby: Disconnecting from old server #{server}"
        sock.close
        @sockets.delete(server)
      end
    end
    # Connect to new servers.
    servers.each do |server|
      if not @sockets[server]
        begin
          Util.logger.info "GearmanRuby: Connecting to server #{server}"
          @sockets[server] = connect(server)
        rescue NetworkError
          @bad_servers << server
          Util.logger.info "GearmanRuby: Unable to connect to #{server}"
        end
      end
    end
  end
  private :update_job_servers

  ##
  # Connect to a job server.
  #
  # @param hostport  "hostname:port"
  def connect(hostport)
    begin
      # FIXME: handle timeouts
      sock = TCPSocket.new(*hostport.split(':'))
    rescue SocketError, Errno::ECONNREFUSED, Errno::ECONNRESET, Errno::EHOSTUNREACH
      raise NetworkError
    rescue Exception => e
      Util.logger.debug "GearmanRuby: Unhandled exception while connecting to #{hostport} : #{e} (raising NetworkError exception)"
      raise NetworkError
    end
    # FIXME: catch exceptions; do something smart
    Util.send_request(sock, Util.pack_request(:set_client_id, @client_id))
    @abilities.each {|f,a| announce_ability(sock, f, a.timeout) }
    sock
  end
  private :connect

  ##
  # Announce an ability over a particular socket.
  #
  # @param sock     Socket connect to a job server
  # @param func     function name (including prefix)
  # @param timeout  the server will give up on us if we don't finish
  #                 a task in this many seconds
  def announce_ability(sock, func, timeout=nil)
    begin
      cmd = timeout ? :can_do_timeout : :can_do
      arg = timeout ? "#{func}\0#{timeout.to_s}" : func
      Util.send_request(sock, Util.pack_request(cmd, arg))
    rescue Exception => ex
      bad_servers << @sockets.keys.detect{|hp| @sockets[hp] == sock}
    end
  end
  private :announce_ability

  ##
  # Add a new ability, announcing it to job servers.
  #
  # The passed-in block of code will be executed for jobs of this function
  # type.  It'll receive two arguments, the data supplied by the client and
  # a Job object.  If it returns nil or false, the server will be informed
  # that the job has failed; otherwise the return value of the block will
  # be passed back to the client in String form.
  #
  # @param func     function name (without prefix)
  # @param timeout  the server will give up on us if we don't finish
  #                 a task in this many seconds
  def add_ability(func, timeout=nil, &f)
    @abilities[func] = Ability.new(f, timeout)
    @sockets.values.each {|s| announce_ability(s, func, timeout) }
  end


  ##
  # Add an after-ability hook
  #
  # The passed-in block of code will be executed after the work block for 
  # jobs with the same function name. It takes two arguments, the result of
  # the work and the original job data. This way, if you need to hook into 
  # *after* the job_complete packet is sent to the server, you can do so. 
  #
  # N.B The after-ability hook ONLY runs if the ability was successful and no 
  # exceptions were raised.
  #
  # @param func     function name (without prefix)
  #
  def after_ability(func, &f)
    @after_abilities[func] = Ability.new(f)
  end
  
  ##
  # Let job servers know that we're no longer able to do something.
  #
  # @param func  function name
  def remove_ability(func)
    @abilities.delete(func)
    req = Util.pack_request(:cant_do, func)
    @sockets.values.each {|s| Util.send_request(s, req) }
  end

  ##
  # Handle a job_assign packet.
  #
  # @param data      data in the packet
  # @param sock      Socket on which the packet arrived
  # @param hostport  "host:port"
  def handle_job_assign(data, sock, hostport)
    handle, func, data = data.split("\0", 3)
    if not func
      Util.logger.error "GearmanRuby: Ignoring job_assign with no function from #{hostport}"
      return false
    end

    Util.logger.error "GearmanRuby: Got job_assign with handle #{handle} and #{data.size} byte(s) " +
      "from #{hostport}"

    ability = @abilities[func]
    if not ability
      Util.logger.error "Ignoring job_assign for unsupported func #{func} " +
        "with handle #{handle} from #{hostport}"
      Util.send_request(sock, Util.pack_request(:work_fail, handle))
      return false
    end

    exception = nil
    begin
      ret = ability.run(data, Job.new(sock, handle))
    rescue Exception => e
      exception = e
      Util.logger.debug "GearmanRuby: Exception: #{e}\n#{e.backtrace.join("\n")}\n"
    end

    cmd = if ret && exception.nil?
      Util.logger.debug "GearmanRuby: Sending work_complete for #{handle} with #{ret.to_s.size} byte(s) " +
        "to #{hostport}"
      [ Util.pack_request(:work_complete, "#{handle}\0#{ret.to_s}") ]
    elsif exception.nil?
      Util.logger.debug "GearmanRuby: Sending work_fail for #{handle} to #{hostport}"
      [ Util.pack_request(:work_fail, handle) ]
    elsif exception
      Util.logger.debug "GearmanRuby: Sending work_exception for #{handle} to #{hostport}"
      [ Util.pack_request(:work_exception, "#{handle}\0#{exception.message}") ]
    end

    cmd.each {|p| Util.send_request(sock, p) }
    
    # There are cases where we might want to run something after the worker
    # successfully completes the ability in question and sends its results
    if ret && exception.nil?
      after_ability = @after_abilities[func]
      if after_ability
        Util.logger.debug "Running after ability for #{func}..."
        begin
          after_ability.run(ret, data)
        rescue Exception => e
          Util.logger.debug "GearmanRuby: Exception: #{e}\n#{e.backtrace.join("\n")}\n"
          nil
        end
      end
    end 
    
    true
  end

  ##
  # Do a single job and return.
  def work
    req = Util.pack_request(:grab_job)
    loop do
      @status = :preparing
      bad_servers = []
      # We iterate through the servers in sorted order to make testing
      # easier.
      servers = nil
      @servers_mutex.synchronize { servers = @sockets.keys.sort }
      servers.each do |hostport|
        Util.logger.debug "GearmanRuby: Sending grab_job to #{hostport}"
        sock = @sockets[hostport]
        Util.send_request(sock, req)

        # Now that we've sent grab_job, we need to keep reading packets
        # until we see a no_job or job_assign response (there may be a noop
        # waiting for us in response to a previous pre_sleep).
        loop do
          begin
            type, data = Util.read_response(sock, @network_timeout_sec)
            case type
            when :no_job
              Util.logger.debug "GearmanRuby: Got no_job from #{hostport}"
              break
            when :job_assign
              @status = :working
              return worker_enabled if handle_job_assign(data, sock, hostport)
              break
            else
              Util.logger.debug "GearmanRuby: Got #{type.to_s} from #{hostport}"
            end
          rescue Exception
            Util.logger.info "GearmanRuby: Server #{hostport} timed out or lost connection (#{$!.inspect}); marking bad"
            bad_servers << hostport
            break
          end
        end
      end

      @servers_mutex.synchronize do
        bad_servers.each do |hostport|
          @sockets[hostport].close if @sockets[hostport]
          @bad_servers << hostport if @sockets[hostport]
          @sockets.delete(hostport)
        end
      end

      Util.logger.debug "GearmanRuby: Sending pre_sleep and going to sleep for #{@reconnect_sec} sec"
      @servers_mutex.synchronize do
        @sockets.values.each do |sock|
          Util.send_request(sock, Util.pack_request(:pre_sleep))
        end
      end

      return false unless worker_enabled
      @status = :waiting

      # FIXME: We could optimize things the next time through the 'each' by
      # sending the first grab_job to one of the servers that had a socket
      # with data in it.  Not bothering with it for now.
      IO::select(@sockets.values, nil, nil, @reconnect_sec)
    end
  end
end

end
