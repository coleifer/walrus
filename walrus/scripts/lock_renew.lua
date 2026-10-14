local key = KEYS[1]
local lock_id = ARGV[1]
local ttl = tonumber(ARGV[2])
if redis.call('get', key) ~= lock_id then
  return 0
end
if ttl > 0 then
  redis.call('pexpire', key, ttl)
end
return 1
