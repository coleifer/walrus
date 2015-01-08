local key = KEYS[1]
local idx = tonumber(ARGV[1])
local arr_len = redis.call('HLEN', key)
if idx < 0 then
  idx = arr_len + idx
end
if idx < 0 or idx >= arr_len then
  return nil
end
local value = redis.call('HGET', key, idx)
while idx < arr_len do
  redis.call('HSET', key, idx, redis.call('HGET', key, idx + 1))
  idx = idx + 1
end
redis.call('HDEL', key, idx - 1)
return value
