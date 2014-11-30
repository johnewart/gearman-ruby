require 'time'

module Gearman
  class TaskSet
    include Logging

    def initialize(client)
      @client = client
      @tasks_in_progress = []
      @finished_tasks = []
    end

    ##
    # Add a new task to this TaskSet.
    #
    # @param args  A Task object
    # @return      true if the task was created successfully, false otherwise
    def add_task(task)
      @tasks_in_progress << task
    end

    ##
    # Wait for all tasks in the set to finish.
    #
    # @param timeout  maximum amount of time to wait, in seconds - if this is nil, waits forever
    def wait(timeout = 1)
      end_time = if timeout
        Time.now.to_f + timeout
      else
        nil
      end

      while not @tasks_in_progress.empty?
        remaining = if end_time
          (t = end_time - Time.now.to_f) > 0 ? t : 0
        else
          nil
        end
        begin
          task = @tasks_in_progress.pop
          if
            @client.submit_job(task, true, remaining)
            @finished_tasks << task
          end
        rescue SocketTimeoutError
          return false
        end

      end

      @finished_tasks.each do |t|
        if ( (t.background.nil? || t.background == false) && !t.successful)
          logger.warn "GearmanRuby: TaskSet failed"
          return false
        end
      end
      true
    end

    # Wait for all tasks in set to finish, with no timeout
    def wait_forever
	wait(nil)
    end

  end

end
