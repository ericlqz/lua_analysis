--
-- sohu lua??????
--
-- User: luoning
-- Date: 14-2-19
-- Time: ??????12:55
--

local http = require("socket.http")
local ltn12 = require("ltn12")
local cjson = require("cjson")
local cjson2 = cjson.new()
local string = require("string")
local table = require("table")
local base64 = require("mime")

--------------------------------------
---- error status
--------------------------------------
-- DOMAIN_URL = "http://api.wanhuatong.tv/lua/getvideo"
DOMAIN_URL = "http://http:192.168.1.65:81/lua/getvideo"
local errorStatus = {
    [0x01] = "get page failed",
    [0x02] = "parse page failed",
    [0x03] = "unknown error",
    [0x04] = "url is not right",
    [0x05] = "timeout",
    [0x06] = "video is not exists",
    [0x07] = "urls is empty",
}

local formatMap = {
    ['ori'] = 'oriVid',
    ['shd'] = 'superVid',
    ['hd'] = 'highVid',
    ['sd'] = 'norVid',
}

local formatMapMy = {
    [1] = 'oriVid',
    [2] = 'superVid',
    [3] = 'highVid',
    [4] = 'norVid',
}

local formatMapMyKey = {
    [1] = 'ori',
    [2] = 'shd',
    [3] = 'hd',
    [4] = 'sd',
}

--------------------------------------
-- user-agent
--------------------------------------
XP_USERAGENT = "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1)"
PHONE_USERAGENT = "Mozilla/5.0 (iPhone; U; CPU iPhone OS 3_0 like Mac OS X; en-us) AppleWebKit/528.18 (KHTML, like Gecko) Version/4.0 Mobile/7A341 Safari/528.16"
IPAD_USERAGENT = "Mozilla/5.0 (iPad; CPU OS 5_1 like Mac OS X) AppleWebKit/534.46 (KHTML, like Gecko) Version/5.1 Mobile/9B176 Safari/7534.48.3"
FIREFOX_USERAGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.7; rv:16.0) Gecko/20100101 Firefox/16.0"

--------------------------------------
-- ??????youku
--------------------------------------
function getByUrl(url)
    local errorMsg = {}
    local t = {}
    --????????????
    local content, status = fetchUrl(url, FIREFOX_USERAGENT)
    if status ~= 200 then
        return { ["code"] = 0x01, ["error"] = errorStatus[0x01] }
    end
    errorMsg['content'] = content
    local first, second = string.find(content, "var vid")
    local first1, second1 = string.find(content, "\n", second)
    if first == nil or first1 == nil then
        return { ["code"] = 0x02, ["error"] = errorStatus[0x02] }, errorMsg
    end
    local vid = string.sub(content, second, first1)
    vid = string.match(vid, "%d+")
    local videoInfo
    if vid == nil or vid == '' or not vid then
        return { ["code"] = 0x02, ["error"] = errorStatus[0x02] }, errorMsg
    end
    local videoUrl, videoContent
    if string.find(url, "http://my") ~= nil then
        videoInfo, videoUrl, videoContent = getMeUrl(vid)
    else
        videoInfo, videoUrl, videoContent = getTvUrl(vid)
    end

    if videoInfo == nil then
        if videoUrl ~= nil then errorMsg['videoUrl'] = videoUrl end
        if videoContent ~= nil then errorMsg['videoContent'] = videoContent end
        return { ["code"] = 0x02, ["error"] = errorStatus[0x02] }, errorMsg
    end
    --????????????????????????
    if not checkUrlsType(videoInfo) then
        return { ["code"] = 0x07, ["error"] = errorStatus[0x07] }, errorMsg
    end

    local res = {}
    res["result"] = videoInfo
    --local iih = cjson2.encode(res)
    --print(iih)
    return { ["code"] = 0, ["content"] = res }
end

--------------------------------------
-- getMeUrl ???????????????my???
--------------------------------------
function getMeUrl(vid)
    local videoUrl = "http://my.tv.sohu.com/videinfo.jhtml?m=viewnew&vid=" .. vid
    local content = fetchUrl(videoUrl, IPAD_USERAGENT)
    local f1,f2 = string.find(content, "data")
    if f1 == nil then
        return nil
    end
    local m3u8 = 'http://my.tv.sohu.com/ipad/00001111110000.m3u8'
    local flag, json = pcall(function ()
        return cjson2.decode(content)
    end)
    if not flag then
        return nil, videoUrl, content
    end

    if not checkIsset(json, "data") then
        return nil
    end
    local res = {}
    for i = 1,4 do
        if checkIsset(json["data"], formatMapMy[i]) then
            if json["data"][formatMapMy[i]] ~= "" and json["data"][formatMapMy[i]] ~= nil then
                res[formatMapMyKey[i]] = string.gsub(m3u8, '00001111110000', vid)
                break
            end
        end
    end
    return res;
end

--------------------------------------
-- getTvUrl ???????????????tv???
--------------------------------------
function getTvUrl(vid)
    local videoUrl = "http://hot.vrs.sohu.com/vrs_flash.action?vid=" .. vid;
    --print("http://hot.vrs.sohu.com/vrs_flash.action?vid=" .. vid);
    local content = fetchUrl(videoUrl, IPAD_USERAGENT)
    local m3u8 = "http://hot.vrs.sohu.com/ipad000111111000.m3u8"
    local flag, json = pcall(function ()
        return cjson2.decode(content)
    end)

    if not flag then
        return nil, videoUrl, content
    end

    local f1,f2 = string.find(content, "data")
    if f1 == nil then
        return nil
    end
    local res = {}
    if not checkIsset(json, "data") then
        return nil
    end
    for i,v in pairs(formatMap) do
        if checkIsset(json['data'], v) then
            if json["data"][v] ~= "" and json["data"][v] ~= nil and json["data"][v] ~= 0 then
                res[i] = string.gsub(m3u8, "000111111000", json["data"][v])
            end
        end
    end
    return res
end
-------------------------------------
--????????????????????????
-------------------------------------
function checkUrlsType(urls)
    if type(urls) ~= 'table' then
        return false
    end
    local i = 0;
    local urlsMaps = {'sd', 'shd', 'hd', 'ori'}
    for _, type in pairs(urlsMaps) do
        if checkIsset(urls, type) then
            i = i + 1;
        end
    end
    if i == 0 then
        return false
    end
    return true;
end

-------------------------------------
--??????table?????????????????????
-------------------------------------
function checkIsset(checkTable, item)
    if type(checkTable) ~= 'table' then
        return false;
    end

    local flag = false
    for key, value in pairs(checkTable) do
        if key == item then
            flag = true
        end
    end
    return flag
end

--------------------------------------
-- ??????url
--------------------------------------
function fetchUrl(url, userAgent, sessioncookie, postData, proxy)
    local t = {}
    --header???
    local headers = {}
    local url = url
    headers["Cookie"] = sessioncookie
    --??????agent
    if userAgent ~= nil then
        headers["User-Agent"] = userAgent
    end

    --postData
    local r, c, h
    if postData == nil then
        local requestData = {
            url = url,
            sink = ltn12.sink.table(t),
            headers = headers,
            method = "GET",
            timeout = 5,
        }
        if proxy ~= nil then
            requestData["proxy"] = proxy
        end
        r, c, h = http.request(requestData)
    else
        headers["Content-Type"] = "application/x-www-form-urlencoded"
        headers["Content-Length"] = string.len(postData)
        local requestData = {
            url = url,
            source = ltn12.source.string(postData),
            sink = ltn12.sink.table(t),
            method = "POST",
            headers = headers,
            timeout = 5,
        }

        if proxy ~= nil then
            requestData["proxy"] = proxy
        end
        r, c, h = http.request(requestData, postData)
    end

    r = table.concat(t, "")
    local cookie
    local location
    if (h) then
        t = {}
        for k, v in pairs(h) do
            --print(k .. "----" .. v)
            if k == "set-cookie" then
                v = string.gsub(v, "(expires=.-; )", "")
                v = v .. ", "
                for cookie in string.gmatch(v, "(.-), ") do
                    cookie = string.match(cookie, "(.-);")
                    table.insert(t, cookie)
                end
            end
            if k == "location" then
                location = v;
            end
        end
        cookie = table.concat(t, "; ")
    else
        cookie = nil
    end
    return r, c, cookie, location
end

--------------------------
--- urlencode
--------------------------
function escapes(s)
    return string.gsub(s, "([^A-Za-z0-9_])", function(c)
        return string.format("%%%02x", string.byte(c))
    end)
end

---------------------------
-- getByUrlJson
---------------------------
function getByUrlJson(params)
    local params = cjson2.decode(params)
    local result, errorMsg = getByUrl(params['url'])
    local res
    local info = {};
    if params['server'] == 1 or params['errorVate'] == 1 then
        if result["code"] == 0 then
            info["type"] = params["type"]
            info["site"] = params["site"]
            info["meid"] = params["meid"]
            info["mid"] = params["mid"]
            info["vate"] = params["server"]
            info["result"] = result["content"]["result"]
        else
            info["msg"] = result["error"]
            info["site"] = params["site"]
            info["url"] = params["url"]
            info["vate"] = params["errorVate"]
            if true and errorMsg ~= nil then
                info["errorMsg"] = base64.b64(cjson2.encode(errorMsg))
            end
        end
        info["domain"] = params["domain"]
        info["code"] = result["code"]
    end
    if result["code"] == 0 then

        local msg = {}
        local i = 1
        for k, v in pairs(result["content"]["result"]) do
            msg[i] = { ["format"] = k, ["url"] = v, ["urlType"] = 0 }
            i = i + 1
        end
        res = { ["code"] = 0, ["msg"] = msg, ["postToServer"] = info }
    else
        res = { ["code"] = result["code"], ["error"] = result["error"], ["postToServer"] = info }
    end
    return cjson2.encode(res)
end

-----------------------------------
-- test
-----------------------------------
--local url = "http://my.tv.sohu.com/pl/5503124/54457652.shtml"
--local url = "http://tv.sohu.com/20140108/n393181556.shtml?txid=8254069965286abe9ee523a73c256ea7"
--result = getByUrl(url)
--local str = '{"meid":"csMCaQPXk5U","mid":"1110IpLQ8n6","url":"http:\/\/tv.sohu.com\/20131202\/n391143278.shtml","server":0,"type":"movie","site":"sohu","errorVate":1,"domain":"http:\/\/api.wanhuatong.tv"}'
--result = getByUrlJson(str)
--print(result)
