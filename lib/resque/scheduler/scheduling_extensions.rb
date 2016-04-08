# vim:fileencoding=utf-8

module Resque
  module Scheduler
    module SchedulingExtensions
      # Accepts a new schedule configuration of the form:
      #
      #   {
      #     "MakeTea" => {
      #       "every" => "1m" },
      #     "some_name" => {
      #       "cron"        => "5/* * * *",
      #       "class"       => "DoSomeWork",
      #       "args"        => "work on this string",
      #       "description" => "this thing works it"s butter off" },
      #     ...
      #   }
      #
      # Hash keys can be anything and are used to describe and reference
      # the scheduled job. If the "class" argument is missing, the key
      # is used implicitly as "class" argument - in the "MakeTea" example,
      # "MakeTea" is used both as job name and resque worker class.
      #
      # Any jobs that were in the old schedule, but are not
      # present in the new schedule, will be removed.
      #
      # :cron can be any cron scheduling string
      #
      # :every can be used in lieu of :cron. see rufus-scheduler's 'every'
      # usage for valid syntax. If :cron is present it will take precedence
      # over :every.
      #
      # :class must be a resque worker class. If it is missing, the job name
      # (hash key) will be used as :class.
      #
      # :args can be any yaml which will be converted to a ruby literal and
      # passed in a params. (optional)
      #
      # :rails_envs is the list of envs where the job gets loaded. Envs are
      # comma separated (optional)
      #
      # :description is just that, a description of the job (optional). If
      # params is an array, each element in the array is passed as a separate
      # param, otherwise params is passed in as the only parameter to
      # perform.
      def schedule=(schedule_hash)
        if supports_lua?
          setup_schedule(schedule_hash)
        else
          Resque.logger.warn!('setup_schedule! can cause a race condition, it is ' \
            'recommended to upgrade your Redis version so that it supports lua SCRIPT')
          setup_schedule!(schedule_hash)
        end
      end

      def setup_schedule!(schedule_hash)
        # This operation tries to be as atomic as possible.
        # It needs to read the existing schedules outside the transaction.
        # Unlikely, but this could still cause a race condition.
        #
        # A more robust solution would be to SCRIPT it, but that would change
        # the required version of Redis.

        # select schedules to remove
        if redis.exists(:schedules)
          clean_keys = non_persistent_schedules
        else
          clean_keys = []
        end

        # Start the transaction. If this is not atomic and more than one
        # process is calling `schedule=` the clean_schedules might overlap a
        # set_schedule and cause the schedules to become corrupt.
        redis.multi do
          clean_schedules(clean_keys)

          schedule_hash = prepare_schedule(schedule_hash)

          # store all schedules in redis, so we can retrieve them back
          # everywhere.
          schedule_hash.each do |name, job_spec|
            set_schedule(name, job_spec)
          end
        end

        # ensure only return the successfully saved data!
        reload_schedule!
      end

      def setup_schedule(schedule_hash)
        schedule = {}

        Resque.redis.evalsha(
          startup_sha,
          keys: [:schedules, :persisted_schedules, :schedules_changed],
          argv: prepare_lua_schedule(schedule_hash)
        ).each_slice(2) do |name, config|
          schedule[name] = decode(config)
        end

        # ensure only return the successfully saved data!
        reload_schedule!(with: schedule)
      end

      # Returns the schedule hash
      def schedule
        @schedule ||= all_schedules
        @schedule || {}
      end

      # reloads the schedule from redis
      def reload_schedule!(with: nil)
        @schedule = with ? with : all_schedules
      end

      # gets the schedules as it exists in redis
      def all_schedules
        return nil unless redis.exists(:schedules)

        redis.hgetall(:schedules).tap do |h|
          h.each do |name, config|
            h[name] = decode(config)
          end
        end
      end

      # clean the schedules as it exists in redis, useful for first setup?
      def clean_schedules(keys = non_persistent_schedules)
        keys.each do |key|
          remove_schedule(key)
        end
        @schedule = nil
        true
      end

      def non_persistent_schedules
        redis.hkeys(:schedules).select { |k| !schedule_persisted?(k) }
      end

      # Create or update a schedule with the provided name and configuration.
      #
      # Note: values for class and custom_job_class need to be strings,
      # not constants.
      #
      #    Resque.set_schedule('some_job', {:class => 'SomeJob',
      #                                     :every => '15mins',
      #                                     :queue => 'high',
      #                                     :args => '/tmp/poop'})
      def set_schedule(name, config)
        persist = config.delete(:persist) || config.delete('persist')
        redis.pipelined do
          redis.hset(:schedules, name, encode(config))
          redis.sadd(:schedules_changed, name)
          redis.sadd(:persisted_schedules, name) if persist
        end
        config
      end

      # retrive the schedule configuration for the given name
      def fetch_schedule(name)
        decode(redis.hget(:schedules, name))
      end

      def schedule_persisted?(name)
        redis.sismember(:persisted_schedules, name)
      end

      # remove a given schedule by name
      def remove_schedule(name)
        redis.hdel(:schedules, name)
        redis.srem(:persisted_schedules, name)
        redis.sadd(:schedules_changed, name)
      end

      private

      def supports_lua?
        redis_master_version >= 2.5
      end

      def redis_master_version
        Resque.redis.info['redis_version'].to_f
      end

      def startup_sha(refresh = false)
        @startup_sha = nil if refresh

        @startup_sha ||=
          Resque.redis.script(:load, <<-EOF.gsub(/^ {14}/, ''))
            -- KEYS
            -- 1: schedules
            -- 2: persisted_schedules
            -- 3: schedules_changed
            -------------------------
            -- ARGV
            -- i:   schedule_name
            -- i+1: encoded_schedule_config
            -- i+2: is_schedule_persisted?

            local remove_schedule = function(schedule)
              redis.call("HDEL", KEYS[1], schedule)
              redis.call("SREM", KEYS[2], schedule)
              redis.call("SADD", KEYS[3], schedule)
            end

            local get_non_persistent_schedules = function()
              local all_schedules = redis.call("HKEYS", KEYS[1])
              local non_persistent_schedules = {}
              for _,schedule in ipairs(all_schedules) do
                local schedule_is_persisted = \
                  redis.call("SISMEMBER", KEYS[2], schedule)
                if schedule_is_persisted == 0 then
                  table.insert(non_persistent_schedules, schedule)
                end
              end
              return non_persistent_schedules
            end

            local set_schedule = function(name, config, persisted)
              redis.call("HSET", KEYS[1], name, config)
              redis.call("SADD", KEYS[3], name)
              if persisted == "true" then
                redis.call("HSET", KEYS[1], name, config)
              end
            end

            local non_persistent_schedules = get_non_persistent_schedules()
            local prepared_schedules = ARGV[1]

            for _,schedule in ipairs(non_persistent_schedules) do
              remove_schedule(schedule)
            end

            for index=1,#ARGV,3 do
             set_schedule(ARGV[index], ARGV[index+1], ARGV[index+2])
            end

            return redis.call("HGETALL", KEYS[1])
          EOF
      end

      def prepare_lua_schedule(schedule_hash)
        prepared_schedule = prepare_schedule(schedule_hash)
        res = []
        prepared_schedule.each do |name, config|
          persist = config.delete(:persist) || config.delete('persist')
          res << name << encode(config) << !persist.nil?
        end
        res
      end

      def prepare_schedule(schedule_hash)
        prepared_hash = {}
        schedule_hash.each do |name, job_spec|
          job_spec = job_spec.dup
          unless job_spec.key?('class') || job_spec.key?(:class)
            job_spec['class'] = name
          end
          prepared_hash[name] = job_spec
        end
        prepared_hash
      end
    end
  end
end
