local key = KEYS[1]
local token = ARGV[1]
local ttl = tonumber(ARGV[2])
local value = tonumber(ARGV[3])
local t = redis.call('time')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
-- Holders are scored by expiry time, so drop any that have expired.
redis.call('zremrangebyscore', key, '-inf', now)
if redis.call('zcard', key) >= value then
  return 0
end
local expires = '+inf'
if ttl > 0 then
  expires = now + ttl
end
redis.call('zadd', key, expires, token)
return 1
