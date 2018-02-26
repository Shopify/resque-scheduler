module Resque
  module Scheduler
    module Lua
      extend self

      def self.zpop(key, *args)
        evalsha(:zpop, [key], args)
      end

      private

      def self.zpop_sha(refresh = false)
        @acquire_sha = nil if refresh

        @acquire_sha ||=
          Resque.redis.script(:load, <<-EOF.gsub(/^ {14}/, ''))
            local items = redis.call('ZRANGEBYSCORE', KEYS[1], ARGV[1], ARGV[2], 'LIMIT', ARGV[3], ARGV[4])

            for k, v in ipairs(items) do
              redis.call('ZREM', KEYS[1], v)
            end -- redis.call('ZREM', KEYS[1], unpack(items))

            return items
          EOF
      end

      def self.evalsha(script, keys, argv, refresh: false)
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
    end
  end
end
