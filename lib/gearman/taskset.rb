#!/usr/bin/env ruby

require 'socket'
require 'time'

module Gearman

# = TaskSet
#
# == Description
# A set of tasks submitted to a Gearman job server.
class TaskSet
  def initialize(client)
    @client = client
    @task_waiting_for_handle = nil
    @tasks_in_progress = {}  # "host:port//handle" -> [job1, job2, ...]
    @finished_tasks = []  # tasks that have completed or failed
    @sockets = {}  # "host:port" -> Socket
    @merge_hash_to_hostport = {}  # Fixnum -> "host:port"
  end

  ##
  # Add a new task to this TaskSet.
  #
  # @param args  either a Task or arguments for Task.new
  # @return      true if the task was created successfully, false otherwise
  def add_task(*args)
    task = Util::get_task_from_args(*args)
    add_task_internal(task, true)
  end

  ##
  # Internal function to add a task.
  #
  # @param task         Task to add
  # @param reset_state  should we reset task state?  true if we're adding a
  #                     new task; false if we're rescheduling one that's
  #                     failed
  # @return             true if the task was created successfully, false
  #                     otherwise
  def add_task_internal(task, reset_state=true)
    task.reset_state if reset_state
    req = task.get_submit_packet(@client.prefix)

    @task_waiting_for_handle = task
    # FIXME: We need to loop here in case we get a bad job server, or the
    # job creation fails (see how the server reports this to us), or ...
    merge_hash = task.get_uniq_hash
    hostport = (@merge_hash_to_hostport[merge_hash] or @client.get_job_server)
    @merge_hash_to_hostport[merge_hash] = hostport if merge_hash
    sock = (@sockets[hostport] or @client.get_socket(hostport))
    Util.log "Using socket #{sock.inspect} for #{hostport}"
    Util.send_request(sock, req)
    while @task_waiting_for_handle
      begin
        read_packet(sock, @client.task_create_timeout_sec)
      rescue NetworkError
        Util.log "Got timeout on read from #{hostport}"
        @task_waiting_for_handle = nil
        @client.close_socket(sock)
        return false
      end
    end

    @sockets[hostport] ||= sock
    true
  end
  private :add_task_internal

  ##
  # Handle a 'job_created' response from a job server.
  #
  # @param hostport  "host:port" of job server
  # @param data      data returned in packet from server
  def handle_job_created(hostport, data)
    Util.log "Got job_created with handle #{data} from #{hostport}"
    if not @task_waiting_for_handle
      raise ProtocolError, "Got unexpected job_created notification " +
        "with handle #{data} from #{hostport}"
    end
    js_handle = Util.handle_to_str(hostport, data)
    task = @task_waiting_for_handle
    @task_waiting_for_handle = nil
    (@tasks_in_progress[js_handle] ||= []) << task
    nil
  end
  private :handle_job_created

  ##
  # Handle a 'work_complete' response from a job server.
  #
  # @param hostport  "host:port" of job server
  # @param data      data returned in packet from server
  def handle_work_complete(hostport, data)
    handle, data = data.split("\0", 2)
    Util.log "Got work_complete with handle #{handle} and " +
      "#{data ? data.size : '0'} byte(s) of data from #{hostport}"
    js_handle = Util.handle_to_str(hostport, handle)
    tasks = @tasks_in_progress.delete(js_handle)
    if not tasks
      raise ProtocolError, "Got unexpected work_complete with handle " +
        "#{handle} from #{hostport} (no task by that name)"
    end
    tasks.each do |t|
      t.handle_completion(data)
      @finished_tasks << t
    end
    nil
  end
  private :handle_work_complete

  ##
  # Handle a 'work_fail' response from a job server.
  #
  # @param hostport  "host:port" of job server
  # @param data      data returned in packet from server
  def handle_work_fail(hostport, data)
    Util.log "Got work_fail with handle #{data} from #{hostport}"
    js_handle = Util.handle_to_str(hostport, data)
    tasks = @tasks_in_progress.delete(js_handle)
    if not tasks
      raise ProtocolError, "Got unexpected work_fail with handle " +
        "#{data} from #{hostport} (no task by that name)"
    end
    tasks.each do |t|
      if t.handle_failure
        add_task_internal(t, false)
      else
        @finished_tasks << t
      end
    end
  end
  private :handle_work_fail

  ##
  # Handle a 'work_status' response from a job server.
  #
  # @param hostport  "host:port" of job server
  # @param data      data returned in packet from server
  def handle_work_status(hostport, data)
    handle, num, den = data.split("\0", 3)
    Util.log "Got work_status with handle #{handle} from #{hostport}: " +
      "#{num}/#{den}"
    js_handle = Util.handle_to_str(hostport, handle)
    tasks = @tasks_in_progress[js_handle]
    if not tasks
      raise ProtocolError, "Got unexpected work_status with handle " +
        "#{handle} from #{hostport} (no task by that name)"
    end
    tasks.each {|t| t.handle_status(num, den) }
  end
  private :handle_work_status

  ##
  # Read and process a packet from a socket.
  #
  # @param sock  socket connected to a job server
  def read_packet(sock, timeout=nil)
    hostport = @client.get_hostport_for_socket(sock)
    if not hostport
      raise RuntimeError, "Client doesn't know host/port for socket " +
        sock.inspect
    end
    type, data = Util.read_response(sock, timeout)
    case type
    when :job_created
      handle_job_created(hostport, data)
    when :work_complete
      handle_work_complete(hostport, data)
    when :work_fail
      handle_work_fail(hostport, data)
    when :work_status
      handle_work_status(hostport, data)
    else
      Util.log "Got #{type.to_s} from #{hostport}"
    end
    nil
  end
  private :read_packet

  ##
  # Wait for all tasks in the set to finish.
  #
  # @param timeout  maximum amount of time to wait, in seconds
  def wait(timeout=1)
    end_time = Time.now.to_f + timeout
    while not @tasks_in_progress.empty?
      remaining = end_time - Time.now.to_f
      ready_socks = IO::select(
        @sockets.values, nil, nil, remaining > 0 ? remaining : 0)
      if not ready_socks or not ready_socks[0]
        Util.log "Timed out while waiting for tasks to finish"
        # not sure what state the connections are in, so just be lame and
        # close them for now
        @sockets.values.each {|s| @client.close_socket(s) }
        @sockets = {}
        return false
      end
      ready_socks[0].each do |sock|
        begin
          read_packet(sock, end_time - Time.now.to_f)
        rescue ProtocolError
          hostport = @client.get_hostport_for_socket(sock)
          Util.log "Ignoring bad packet from #{hostport}"
        rescue NetworkError
          hostport = @client.get_hostport_for_socket(sock)
          Util.log "Got timeout on read from #{hostport}"
        end
      end
    end
    @sockets.values.each {|s| @client.return_socket(s) }
    @sockets = {}
    @finished_tasks.each do |t|
      if not t.successful
        Util.log "Taskset failed"
        return false
      end
    end
    true
  end
end

end
