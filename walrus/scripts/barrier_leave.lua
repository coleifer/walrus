local key = KEYS[1]
local gen = tonumber(ARGV[2])
local current = tonumber(redis.call('hget', key, 'gen') or 0)
if current > gen then
  return 0 -- Released before the caller gave up.
elseif current == gen then
  redis.call('hincrby', key, 'count', -1)
end
return 1
