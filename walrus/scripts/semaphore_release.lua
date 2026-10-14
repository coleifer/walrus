local key = KEYS[1]
local event_key = KEYS[2]
local token = ARGV[1]
local value = tonumber(ARGV[3])
local t = redis.call('time')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
redis.call('zremrangebyscore', key, '-inf', now)
if redis.call('zrem', key, token) == 1 then
  redis.call('lpush', event_key, 1)
  redis.call('ltrim', event_key, 0, value - 1) -- One signal per slot.
  return 1
end
return 0
