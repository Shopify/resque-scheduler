# vim:fileencoding=utf-8

module Resque
  module Scheduler
    module Lock
      class Base
        attr_reader :key
        attr_accessor :timeout

        def initialize(key, options = {})
          @key = key

          # 3 minute default timeout
          @timeout = options[:timeout] || 60 * 3
        end

        # Attempts to acquire the lock. Returns true if successfully acquired.
        def acquire!
          raise NotImplementedError
        end

        def value
          [hostname, process_id].join(':')
        end

        # Returns true if you currently hold the lock.
        def locked?
          raise NotImplementedError
        end

        # Releases the lock iff we own it
        def release_if_locked
          raise NotImplementedError
        end

        private

        # Releases the lock unconditionally
        def release!
          Resque.redis.del(key) == 1
        end

        # Extends the lock by `timeout` seconds.
        def extend_lock!
          Resque.redis.expire(key, timeout)
        end

        def hostname
          local_hostname = Socket.gethostname
          @hostname ||= begin
            Socket.gethostbyname(local_hostname).first
          rescue
            local_hostname
          end
        end

        def process_id
          Process.pid
        end
      end
    end
  end
end
