local items = redis.call('ZRANGEBYSCORE', KEYS[1], ARGV[1], ARGV[2], 'LIMIT', ARGV[3], ARGV[4])

for k, v in ipairs(items) do
  redis.call('ZREM', KEYS[1], v)
end

return items
