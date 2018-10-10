-- Copyright (C) Kong Inc.

local BasePlugin = require "kong.plugins.base_plugin"
local access = require "kong.plugins.response-ratelimiting.access"
local log = require "kong.plugins.response-ratelimiting.log"
local header_filter = require "kong.plugins.response-ratelimiting.header_filter"


local ResponseRateLimitingHandler = BasePlugin:extend()


function ResponseRateLimitingHandler:new()
  ResponseRateLimitingHandler.super.new(self, "response-ratelimiting")
end


function ResponseRateLimitingHandler:access(conf)
  ResponseRateLimitingHandler.super.access(self)
  access.execute(conf)
end


function ResponseRateLimitingHandler:header_filter(conf)
  ResponseRateLimitingHandler.super.header_filter(self)
  header_filter.execute(conf)
end


function ResponseRateLimitingHandler:log(conf)
  if not kong.ctx.plugin.stop_log and kong.ctx.plugin.usage then
    ResponseRateLimitingHandler.super.log(self)
    log.execute(conf, kong.ctx.plugin.identifier, kong.ctx.plugin.current_timestamp, kong.ctx.plugin.increments, kong.ctx.plugin.usage)
  end
end


ResponseRateLimitingHandler.PRIORITY = 900
ResponseRateLimitingHandler.VERSION = "0.1.0"

return ResponseRateLimitingHandler
