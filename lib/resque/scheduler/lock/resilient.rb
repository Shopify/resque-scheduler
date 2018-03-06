# vim:fileencoding=utf-8
require_relative 'base'

module Resque
  module Scheduler
    module Lock
      class Resilient < Base
        def acquire!
          Resque.redis.set(key, value, ex: timeout, nx: true)
        end

        def locked?
          Resque::Scheduler::Lua.locked(key, value, @timeout).to_i == 1
        end

        def release_if_locked
          Resque::Scheduler::Lua.deleq(key, value).to_i == 1
        end

        def timeout=(seconds)
          if locked?
            @timeout = seconds
          end
          @timeout
        end
      end
    end
  end
end
