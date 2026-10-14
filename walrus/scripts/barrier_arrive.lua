local key = KEYS[1]
local parties = tonumber(ARGV[1])
if redis.call('exists', key) == 0 then
  -- A new barrier, or one whose hash was lost, starts with no signals.
  redis.call('del', KEYS[2], KEYS[3])
end
local gen = tonumber(redis.call('hget', key, 'gen') or 0)
if redis.call('hincrby', key, 'count', 1) < parties then
  return {gen, 0}
end
-- Last to arrive: start the next generation and release this one. The next
-- generation's signal list is emptied before anyone can wait on it.
redis.call('hset', key, 'count', 0)
redis.call('hset', key, 'gen', gen + 1)
redis.call('del', KEYS[2 + (gen + 1) % 2])
redis.call('lpush', KEYS[2 + gen % 2], 1)
return {gen, 1}
