local key = KEYS[1]
local token = ARGV[1]
local ttl = tonumber(ARGV[2])
local t = redis.call('time')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
redis.call('zremrangebyscore', key, '-inf', now)
if not redis.call('zscore', key, token) then
  return 0
end
if ttl > 0 then
  redis.call('zadd', key, now + ttl, token)
end
return 1
