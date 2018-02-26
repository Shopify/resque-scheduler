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
          evalsha(:locked, [key], [value]).to_i == 1
        end

        def timeout=(seconds)
          if locked?
            @timeout = seconds
            @locked_sha = nil
          end
          @timeout
        end

        private

        def evalsha(script, keys, argv, refresh: false)
          sha_method_name = "#{script}_sha"
          Resque.redis.evalsha(
            send(sha_method_name, refresh),
            keys: keys,
            argv: argv
          )
        rescue Redis::CommandError => e
          if e.message =~ /NOSCRIPT/
            refresh = true
            retry
          end
          raise
        end

        def locked_sha(refresh = false)
          @locked_sha = nil if refresh

          @locked_sha ||=
            Resque.redis.script(:load, <<-EOF.gsub(/^ {14}/, ''))
              if redis.call('GET', KEYS[1]) == ARGV[1]
              then
                redis.call('EXPIRE', KEYS[1], #{timeout})

                if redis.call('GET', KEYS[1]) == ARGV[1]
                then
                  return 1
                end
              end

              return 0
            EOF
        end
      end
    end
  end
end
