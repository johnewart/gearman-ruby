module Gearman
  class Worker

    class Ability
      ##
      # Create a new ability. Setting timeout means we register with CAN_DO_TIMEOUT
      # @param func_name  Function name of this ability
      # @param block      Code to run
      # @param timeout    Server gives up on us after this many seconds
      def initialize(func_name, block, timeout=nil)
        @func_name = func_name
        @block = block
        @timeout = timeout
        @on_complete = nil
      end

      attr_reader :timeout, :func_name

      ##
      # Run the block of code given for a job of this type.
      #
      # @param data  data passed to us by a client
      # @param job   interface to report job information to the server
      def run(data, job)
        begin
          result = @block.call(data, job) if @block
          @on_complete.call(result, data) if @on_complete
          return result
        rescue => ex
          raise ex
        end
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
      def after_complete(&block)
        @on_complete = block
      end


    end

  end
end
