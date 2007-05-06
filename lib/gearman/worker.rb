#!/usr/bin/ruby

require 'set'
require 'socket'

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
  # Number of seconds to sleep when we don't have work before polling the
  # job server again (if a job comes in while we're sleeping, the server
  # will wake us up).
  SLEEP_SEC = 10

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
  end

  ##
  # Create a new worker.
  #
  # @param job_servers  "host:port"; either a single server or an array
  # @param prefix       function name prefix (namespace)
  def initialize(job_servers=nil, prefix=nil)
    chars = ('a'..'z').to_a
    @id = Array.new(30) { chars[rand(chars.size)] }.join
    @sockets = {}
    @abilities = {}
    self.job_servers = job_servers if job_servers
    @prefix = prefix
  end

  ##
  # Connect to job servers to be used by this worker.
  #
  # @param servers  "host:port"; either a single server or an array
  def job_servers=(servers)
    servers = Set.new(Util.normalize_job_servers(servers))
    # Disconnect from servers that we no longer care about.
    @sockets.each do |server,sock|
      if not servers[server]
        sock.disconnect
        @sockets.delete(server)
      end
    end
    # Connect to new servers.
    servers.each do |server|
      if not @sockets[server]
        @sockets[server] = connect(server)
      end
    end
  end

  ##
  # Connect to a job server.
  #
  # @param hostport  "hostname:port"
  def connect(hostport)
    sock = TCPSocket.new(*hostport.split(':'))
    # FIXME: catch exceptions; do something smart
    Util.send_request(sock, Util.pack_request(:set_client_id, @id))
    @abilities.each {|f,a| announce_ability(sock, f, a.timeout) }
    @sockets[hostport] = sock
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
    cmd = timeout ? :can_do_timeout : :can_do
    arg = timeout ? "#{func}\0#{timeout.to_s}" : func
    Util.send_request(sock, Util.pack_request(cmd, arg))
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
    func = (@prefix ? "#{@prefix}\t" : '') + func
    @abilities[func] = Ability.new(f, timeout)
    @sockets.values.each {|s| announce_ability(s, func, timeout) }
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
  # @param data  data in the packet
  # @param sock  Socket on which the packet arrived
  def handle_job_assign(data, sock)
    handle, func, data = data.split("\0", 3)
    if not func
      Util.err "Ignoring job_assign with no function"
      return false
    end

    Util.log "Got job_assign with handle #{handle} and #{data.size} byte(s)"

    ability = @abilities[func]
    if not ability
      Util.err "Ignoring job_assign for unsupported func #{func} " +
        "with handle #{handle}"
      Util.send_request(sock, Util.pack_request(:work_fail, handle))
      return false
    end

    ret = ability.run(data, Job.new(sock, handle))

    cmd = nil
    if ret
      ret = ret.to_s
      Util.log "Sending work_complete for #{handle} with #{ret.size} byte(s)"
      cmd = Util.pack_request(:work_complete, "#{handle}\0#{ret}")
    else
      Util.log "Sending work_fail for #{handle}"
      cmd = Util.pack_request(:work_fail, handle)
    end

    Util.send_request(sock, cmd)
    true
  end

  ##
  # Do a single job and return.
  def work
    loop do
      @sockets.values.each do |sock|
        Util.log "Sending grab_job"
        Util.send_request(sock, Util.pack_request(:grab_job))
        # Now that we've sent grab_job, we need to keep reading packets
        # until we see a no_job or job_assign response (there may be a noop
        # waiting for us in response to a previous pre_sleep).
        loop do
          type, data = Util.read_response(sock)
          case type
          when :noop
            Util.log "Got noop"
            next
          when :no_job
            Util.log "Got no_job"
            break
          when :job_assign
            return if handle_job_assign(data, sock)
          else
            Util.log "Got #{type.to_s}"
          end
        end
      end
      Util.log "Sending pre_sleep and going to sleep for #{SLEEP_SEC} sec"
      @sockets.values.each do |sock|
        Util.send_request(sock, Util.pack_request(:pre_sleep))
      end
      IO::select(@sockets.values, nil, nil, SLEEP_SEC)
    end
  end
end

end
