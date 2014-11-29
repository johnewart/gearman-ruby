module Gearman

  class InvalidArgsError < Exception
  end

  class ProtocolError < Exception
  end

  class NetworkError < Exception
  end

  class NoJobServersError < Exception
  end

  class JobQueueError < Exception
  end

  class SocketTimeoutError < Exception
  end

  class ServerDownException < Exception
  end

end