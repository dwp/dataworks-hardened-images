local req_headers = ngx.req.get_headers()
local token = (req_headers["Authorization"]
            and req_headers["Authorization"]
            or ngx.var.arg_token)
local user = (req_headers["Username"]
            and req_headers["Username"]
            or "NOT_SET")

local jwt = check_jwt(token, user)
substitute_proxy_user(jwt)
remove_token_query_param()
