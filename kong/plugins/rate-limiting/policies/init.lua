local policy_cluster = require "kong.plugins.rate-limiting.policies.cluster"
local singletons = require "kong.singletons"
local reports = require "kong.reports"
local redis = require "resty.redis"
local luatz = require "luatz"


local timetable = luatz.timetable
local pairs = pairs
local floor = math.floor
local null = ngx.null
local shm = ngx.shared.kong_rate_limiting_counters
local fmt = string.format


local log_err = kong and kong.log.err or function(...)
  return ngx.log(ngx.ERR, ...)
end


local EMPTY_UUID = "00000000-0000-0000-0000-000000000000"


local ms_check = timetable.new(20000, 1, 1, 0, 0, 0):timestamp()

local function get_timetable(now)
  if now > ms_check then
    return timetable.new_from_timestamp(now / 1000)
  end

  return timetable.new_from_timestamp(now)
end


local function get_timestamps(now)
  local timetable = get_timetable(now)
  local stamps = {}

  timetable.sec = floor(timetable.sec)   -- reduce to second precision
  stamps.second = timetable:timestamp() * 1000

  timetable.sec = 0
  stamps.minute = timetable:timestamp() * 1000

  timetable.min = 0
  stamps.hour = timetable:timestamp() * 1000

  timetable.hour = 0
  stamps.day = timetable:timestamp() * 1000

  timetable.day = 1
  stamps.month = timetable:timestamp() * 1000

  timetable.month = 1
  stamps.year = timetable:timestamp() * 1000

  return stamps
end


local function is_present(str)
  return str and str ~= "" and str ~= null
end


local function get_ids(conf)
  conf = conf or {}

  local api_id = conf.api_id

  if api_id and api_id ~= null then
    return EMPTY_UUID, EMPTY_UUID, api_id
  end

  api_id = EMPTY_UUID

  local service_id = conf.service_id
  local route_id   = conf.route_id

  if not service_id or service_id == null then
    service_id = EMPTY_UUID
  end

  if not route_id or route_id == null then
    route_id = EMPTY_UUID
  end

  return service_id, route_id, api_id
end


local get_local_key = function(conf, identifier, period, period_date)
  local service_id, route_id, api_id = get_ids(conf)

  if api_id == EMPTY_UUID then
    return fmt("ratelimit:%s:%s:%s:%s:%s", route_id, service_id, identifier, period_date, period)
  end

  return fmt("ratelimit:%s:%s:%s:%s", api_id, identifier, period_date, period)
end


local EXPIRATIONS = {
  second = 1,
  minute = 60,
  hour   = 3600,
  day    = 86400,
  month  = 2592000,
  year   = 31536000,
}


return {
  ["local"] = {
    increment = function(conf, limits, identifier, current_timestamp, value)
      local periods = get_timestamps(current_timestamp)
      for period, period_date in pairs(periods) do
        if limits[period] then
          local cache_key = get_local_key(conf, identifier, period_date, period)
          local newval, err = shm:incr(cache_key, value, 0)
          if not newval then
            log_err("could not increment counter for period '", period, "': ", err)
            return nil, err
          end
        end
      end

      return true
    end,
    usage = function(conf, identifier, period, current_timestamp)
      local periods = get_timestamps(current_timestamp)
      local cache_key = get_local_key(conf, identifier, periods[period], period)
      local current_metric, err = shm:get(cache_key)
      if err then
        return nil, err
      end
      return current_metric or 0
    end
  },
  ["cluster"] = {
    increment = function(conf, limits, identifier, current_timestamp, value)
      local db = singletons.dao.db
      local service_id, route_id, api_id = get_ids(conf)

      local ok, err

      if api_id == EMPTY_UUID then
        ok, err = policy_cluster[db.name].increment(db, limits, identifier, current_timestamp,
                                                    service_id, route_id, value)

      else
        ok, err = policy_cluster[db.name].increment_api(db, limits, identifier, current_timestamp,
                                                        api_id, value)
      end

      if not ok then
        log_err("cluster policy: could not increment ", db.name, " counter: ", err)
      end

      return ok, err
    end,
    usage = function(conf, identifier, period, current_timestamp)
      local db = singletons.dao.db
      local service_id, route_id, api_id = get_ids(conf)
      local row, err

      if api_id == EMPTY_UUID then
        row, err = policy_cluster[db.name].find(db, identifier, period, current_timestamp,
                                                service_id, route_id)
      else
        row, err = policy_cluster[db.name].find_api(db, identifier, period, current_timestamp,
                                                    api_id)
      end

      if err then
        return nil, err
      end

      if row and row.value ~= null and row.value > 0 then
        return row.value
      end

      return 0
    end
  },
  ["redis"] = {
    increment = function(conf, limits, identifier, current_timestamp, value)
      local red = redis:new()
      red:set_timeout(conf.redis_timeout)
      local ok, err = red:connect(conf.redis_host, conf.redis_port)
      if not ok then
        log_err("failed to connect to Redis: ", err)
        return nil, err
      end

      local times, err = red:get_reused_times()
      if err then
        log_err("failed to get connect reused times: ", err)
        return nil, err
      end

      if times == 0 and is_present(conf.redis_password) then
        local ok, err = red:auth(conf.redis_password)
        if not ok then
          log_err("failed to auth Redis: ", err)
          return nil, err
        end
      end

      if times ~= 0 or conf.redis_database then
        -- The connection pool is shared between multiple instances of this
        -- plugin, and instances of the response-ratelimiting plugin.
        -- Because there isn't a way for us to know which Redis database a given
        -- socket is connected to without a roundtrip, we force the retrieved
        -- socket to select the desired database.
        -- When the connection is fresh and the database is the default one, we
        -- can skip this roundtrip.

        local ok, err = red:select(conf.redis_database or 0)
        if not ok then
          log_err("failed to change Redis database: ", err)
          return nil, err
        end
      end

      local keys = {}
      local expirations = {}
      local idx = 0
      local periods = get_timestamps(current_timestamp)
      for period, period_date in pairs(periods) do
        if limits[period] then
          local cache_key = get_local_key(conf, identifier, period_date, period)
          local exists, err = red:exists(cache_key)
          if err then
            log_err("failed to query Redis: ", err)
            return nil, err
          end

          idx = idx + 1
          keys[idx] = cache_key
          if not exists or exists == 0 then
            expirations[idx] = EXPIRATIONS[period]
          end
        end
      end

      red:init_pipeline()
      for i = 1, idx do
        red:incrby(keys[i], value)
        if expirations[i] then
          red:expire(keys[i], expirations[i])
        end
      end

      local _, err = red:commit_pipeline()
      if err then
        log_err("failed to commit pipeline in Redis: ", err)
        return nil, err
      end

      local ok, err = red:set_keepalive(10000, 100)
      if not ok then
        log_err("failed to set Redis keepalive: ", err)
        return nil, err
      end

      return true
    end,
    usage = function(conf, identifier, period, current_timestamp)
      local red = redis:new()

      red:set_timeout(conf.redis_timeout)

      local ok, err = red:connect(conf.redis_host, conf.redis_port)
      if not ok then
        log_err("failed to connect to Redis: ", err)
        return nil, err
      end

      local times, err = red:get_reused_times()
      if err then
        log_err("failed to get connect reused times: ", err)
        return nil, err
      end

      if times == 0 and is_present(conf.redis_password) then
        local ok, err = red:auth(conf.redis_password)
        if not ok then
          log_err("failed to connect to Redis: ", err)
          return nil, err
        end
      end

      if times ~= 0 or conf.redis_database then
        -- The connection pool is shared between multiple instances of this
        -- plugin, and instances of the response-ratelimiting plugin.
        -- Because there isn't a way for us to know which Redis database a given
        -- socket is connected to without a roundtrip, we force the retrieved
        -- socket to select the desired database.
        -- When the connection is fresh and the database is the default one, we
        -- can skip this roundtrip.

        local ok, err = red:select(conf.redis_database or 0)
        if not ok then
          log_err("failed to change Redis database: ", err)
          return nil, err
        end
      end

      reports.retrieve_redis_version(red)

      local periods = get_timestamps(current_timestamp)
      local cache_key = get_local_key(conf, identifier, period, periods[period])

      local current_metric, err = red:get(cache_key)
      if err then
        return nil, err
      end

      if current_metric == null then
        current_metric = nil
      end

      local ok, err = red:set_keepalive(10000, 100)
      if not ok then
        log_err("failed to set Redis keepalive: ", err)
      end

      return current_metric or 0
    end
  }
}
