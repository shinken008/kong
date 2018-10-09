local BasePlugin = require "kong.plugins.base_plugin"
local singletons = require "kong.singletons"
local constants = require "kong.constants"
local meta = require "kong.meta"


local ngx = ngx
local kong = kong
local server_header = meta._SERVER_TOKENS


local DEFAULT_RESPONSE = {
  [401] = "Unauthorized",
  [404] = "Not found",
  [405] = "Method not allowed",
  [500] = "An unexpected error occurred",
  [502] = "Bad Gateway",
  [503] = "Service unavailable",
}


local RequestTerminationHandler = BasePlugin:extend()


RequestTerminationHandler.PRIORITY = 2
RequestTerminationHandler.VERSION = "0.1.2"


local function flush(ctx)
  local response = ctx.delayed_response or kong.ctx.shared.delayed_response or
                   ngx.ctx.delayed_response

  local status       = response.status_code
  local content      = response.content
  local content_type = response.content_type
  if not content_type then
    content_type = "application/json; charset=utf-8";
  end

  ngx.status = status

  if singletons.configuration.enabled_headers[constants.HEADERS.SERVER] then
    kong.response.set_header(constants.HEADERS.SERVER, server_header)

  else
    kong.response.clear_header(constants.HEADERS.SERVER)
  end

  kong.response.set_header("Content-Type", content_type)
  kong.response.set_header("Content-Length", #content)

  ngx.print(content)

  return ngx.exit(status)
end


function RequestTerminationHandler:new()
  RequestTerminationHandler.super.new(self, "request-termination")
end


function RequestTerminationHandler:access(conf)
  RequestTerminationHandler.super.access(self)

  local status  = conf.status_code
  local content = conf.body

  if content then
    local shared_ctx = kong.ctx.shared
    local ngx_ctx = ngx.ctx

    if (shared_ctx.delay_response and not shared_ctx.delayed_response) or
       (ngx_ctx.delay_response    and not ngx_ctx.delayed_response) then

      local delayed_response = {
        status_code  = status,
        content      = content,
        content_type = conf.content_type,
      }

      shared_ctx.delayed_response = delayed_response
      shared_ctx.delayed_response_callback = flush

      ngx_ctx.delayed_response = delayed_response
      ngx_ctx.delayed_response_callback = flush

      return
    end
  end

  return kong.response.exit(status, { message = conf.message or DEFAULT_RESPONSE[status] })
end


return RequestTerminationHandler
