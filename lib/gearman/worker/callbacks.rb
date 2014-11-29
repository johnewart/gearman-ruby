# Provides callbacks for internal worker use:
#
# def named_metric(metric)
#   "HardWorker.#{Process.pid}.#{metric}"
# end
#
# worker = Gearman::Worker.new
# worker.on_grab_job { StatsD.increment(named_metric('grab_job')) }
# worker.on_job_assign { StatsD.increment(named_metric('job_assign')) }
# worker.on_no_job { StatsD.increment(named_metric('no_job')) }
# worker.on_work_complete { StatsD.increment(named_metric('work_complete')) }

module Gearman
  class Worker

    module Callbacks

      %w(connect grab_job no_job job_assign work_complete work_fail
         work_exception).each do |event|

        define_method("on_#{event}") do |&callback|
          instance_variable_set("@__on_#{event}", callback)
        end

        define_method("run_#{event}_callback") do
          callback = instance_variable_get("@__on_#{event}")
          return unless callback

          begin
            callback.call
          rescue Exception => e
            logger.error "#{event} failed: #{e.inspect}"
          end
        end
      end
    end

  end
end