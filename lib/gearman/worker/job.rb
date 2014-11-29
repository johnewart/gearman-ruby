module Gearman
  class Worker

    class Job
      ##
      # Create a new Job.
      #
      # @param sock    Socket connected to job server
      # @param handle  job server-supplied job handle
      attr_reader :handle

      def initialize(connection, handle)
        @connection = connection
        @handle = handle
      end

      ##
      # Report our status to the job server.
      def report_status(numerator, denominator)
        req = Packet.pack_request(:work_status, "#{@handle}\0#{numerator}\0#{denominator}")
        @connection.send_update(req)
        self
      end

      ##
      # Send data before job completes
      def send_data(data)
        req = Packet.pack_request(:work_data, "#{@handle}\0#{data}")
        @connection.send_update(req)
        self
      end

      ##
      # Send a warning explicitly
      def report_warning(warning)
        req = Packet.pack_request(:work_warning, "#{@handle}\0#{warning}")
        @connection.send_update(req)
        self
      end
    end

  end
end

