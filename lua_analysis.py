#!/usr/bin/env python2
# encoding: utf-8

import httplib2
import json
import sqlite3
import time
import datetime
import re

# TODO: 网盘源特殊分析处理
# TODO: 交叉分析
# TODO: site -s 源(影片列表) -c 源(某错误的影片列表)
# TODO: url -r 地址 -u (某链接错误的用户分布)
# TODO: user -u 用户 -s(用户在各源的分布) -c(在各错误码的分布)
# TODO: 时间段?
# TODO: 影响抓取的因素
# TODO: 各个源的前端抓取策略
# TODO: 哪个错误代码，客户端重试
# TODO: 哪些错误代码，反馈给服务端(或人工验证) 1, 6, 7 VS FastCheck
# TODO: 客户端日志模块
# TODO: 各类型影片数据量估计
# TODO: 收费源后端处理(客户端报错、服务端缓存): 热门影片 vs 网盘?
# TODO: 所谓时效
# TODO: 抓取地址: 带IP/不带IP
# TODO: 单影片报警 VS 源报警

import sys
reload(sys)
sys.setdefaultencoding('UTF8')


class Analysis:

    """请求源的名称"""
    sources = [
        u'tudou', u'wole56', u'sohu',
        u'youku', u'cntv', u'm1905', u'letv',
        u'qiyi', u'qq', u'fengxing', u'pps',
        u'bps', u'pptv', u'kankan', u'tv189',
        u'baofeng'
    ]
    # u'tudou', u'wole56', u'sina', u'sohu',

    """请求源的日志地址的链接前缀"""
    source_prefix = u"http://api.wanhuatong.tv/lua/geterror?site="

    """所有错误码"""
    error_codes = [
        "1", "2", "6", "7", "32", "33", "34", "37"
    ]
    #"1", "2", "6", "32", "33", "34", "35", "37"
    #"35":"script time out",

    """ 暂时忽略计算的错误码 """
    ignore_codes = ["35"]

    """有错误码的描述"""
    error_desc = {
        "1":"get page failed",
        "2":"parse page failed",
        "6":"video not exists",
        "7":"msg is empty",
        "32":"script run crash",
        "33":"can not found lua script",
        "34":"crawl result invalid",
        "37":"script decrypt error"
    }

    def __init__(self, options, args, basetime):
        # 实例化数据库工具
        self.dbHelper = DBHelper()
        # 实例化时间工具
        self.timeUtil = TimeUtil()
        # 命令行参数
        self.options = options
        # 命令行参数
        self.args= args
        # 查询的时间点，若用户无指定，则在代码中指定
        self.basetime = basetime
        # 当前所有日志的总数
        self.log_total_count = 0
        # 该次数据同步添加的日志数
        self.appendItemCount = 0
        # 数据同步错误日志
        self.log_file_name = "sync_log"
        # 默认查询参数
        self.conditions = []
        self.conditions.extend(self.getTimespanCondition())
        lua_version_condition = self.getLuaVersionCondition()
        if lua_version_condition:
            self.conditions.append(lua_version_condition)

    def startWork(self):
        """ 根据命令行参数决定运行 """
        func_map = {
            "sync": self.syncData,
            "analysis": self.analysis,
            "sql": self.runQuery,
            "error": self.calError
        }

        action = self.args[0]
        func = func_map.get(action, None)

        if func: func()

    def syncData(self):
        """ 同步Lua抓取的后台日志 """
        lastTime = self.getLastTime()
        print "lastTime: ", lastTime

        failed_sites = []

        designated_site = self.options.site
        sync_site_names = (designated_site,) if designated_site else self.sources

        h = httplib2.Http(".cache")
        for sourceName in sync_site_names:
            print "start source: ", sourceName
            resp, content = h.request(self.source_prefix + sourceName)

            # 若请求失败，尝试重试
            retry = 0
            while not content and retry > 2:
                print "Failed ", sourceName, " Retry"
                resp, content = h.request(self.source_prefix + sourceName)
                retry += 1

            # 重试仍失败，则跳过该网站
            if not content:
                failed_sites.append(sourceName)
                continue

            # 存储数据
            self.parseData(sourceName, content, lastTime)

        print "Finish Sync Data."
        print "Append ", self.appendItemCount, " Items. Total Items Count: ", self.getTotalItemCount()
        print "Failed Sites: ", failed_sites

        if failed_sites: self.logToFile(",".join(failed_sites))

    def logToFile(self, log):
        """ 写入日志信息 """
        if log:
            log_file = open(self.log_file_name, 'a')
            log_file.write("Error: " + log)
            log_file.close()

    def getLastTime(self):
        """ 获取最近一条日志的时间 """
        queryStr = "select max(uploadTime) from item"
        return self.dbHelper.queryTop(queryStr)

    def getEarlyTime(self):
        """ 获取最早一条日志的时间 """
        queryStr = "select min(uploadTime) from item where uploadTime != ''"
        return self.dbHelper.queryTop(queryStr)

    def parseData(self, sourceName, content, lastTime):
        """ 数据解析、存储 """
        try:
            msgObj = json.loads(content)
            msgMsg = msgObj.get("msg")
            if msgMsg:
                for itemObj in msgMsg:
                    self.storeItem(lastTime, itemObj)
                print sourceName, "current has log:", len(msgMsg)
        except Exception, ex:
            print "source: ", sourceName, ex.message

    def storeItem(self, lastTime, data):
        """ 存储日志 """
        item = {}
        item["site"] = data.get("site", "")
        item["code"] = data.get("code", 0)
        item["uuid"] = data.get("uuid", "")
        item["msg"]  = data.get("msg", "")
        item["url"]  = data.get("url", "")
        item["version"] = data.get("version", "")
        item["lua_version"] = data.get("lua_version", 0)
        item["uploadTime"] = data.get("uploadTime", "")

        if item["uploadTime"] > lastTime:
            print "Store new item: ", item
            self.dbHelper.store(item)
            self.appendItemCount += 1

    def getUserCount(self):
        """ 计算用户总数 """
        queryStr = "select count(*) from item group by uuid"
        uuid_counts = self.dbHelper.query(queryStr)

        if uuid_counts:
            return len(uuid_counts)

    def getTotalItemCount(self, conditions = None):
        """ 根据条件，计算日志的总数 """
        queryStr = "select count(*) from item"
        queryStr = self.addAndConditionsToStr(conditions, queryStr)

        if self.options.debug:
            print 'queryStr_total: ', queryStr

        return self.dbHelper.queryTop(queryStr)

    def getTotalErrorItemCount(self, conditions = None):
        """ 计算错误日志的总数 """
        queryStr_total_error = "select count(*) from item"
        queryStr_total_error = self.addAndConditionsToStr(conditions, queryStr_total_error)
        queryStr_total_error += " and code != 0"

        for ignore_code in self.ignore_codes:
            queryStr_total_error += " and code != " + ignore_code

        if self.options.debug:
            print 'queryStr_total_error: ', queryStr_total_error
        return self.dbHelper.queryTop(queryStr_total_error)

    def analysis(self):
        pass

    def runQuery(self, query = None):
        """ 执行SQL语句 """
        queryStr = query if query else self.options.sql

        if self.options.debug:
            print "runQuery: ", queryStr

        if queryStr:
            results = self.dbHelper.query(queryStr)
            for result in results:
                print result

    def calError(self):
        """
        总结计算错误情况.
        state: 整体错误分布
        site: 计算各源错误分布
        increment: 计算每日新增错误
        """
        side = self.options.side

        if side == 'increment':
            self.calNewErrorDistribute()
        elif side == 'all':
            self.calStateDistribute()
        elif side == 'site':
            self.calSiteErrorDistribute()

    def calStateDistribute(self):
        """ 计算整体错误分布 """
        error_where = " code != 0 and code != 35 "
        timespan = self.getTimespan()

        log_error_str = self.addTimespanToStr("select count(*) from item where " + error_where, timespan)
        log_all_str = self.addTimespanToStr("select count(*) from item", timespan, first=True)
        user_error_str = self.addTimespanToStr("select count(distinct uuid) from item where " + error_where, timespan)
        user_all_str = self.addTimespanToStr("select count(distinct uuid) from item", timespan, first=True)
        url_error_str = self.addTimespanToStr("select count(distinct url) from item where " + error_where, timespan)
        url_all_str = self.addTimespanToStr("select count(distinct url) from item", timespan, first=True)

        if self.options.debug:
            print 'log_error_str:', log_error_str
            print 'log_all_str:', log_all_str
            print 'user_error_str:', user_error_str
            print 'user_all_str:', user_all_str
            print 'url_error_str:', url_error_str
            print 'url_all_str:', url_all_str

        print "日志(出错/所有):", self.dbHelper.queryTop(log_error_str), "/", self.dbHelper.queryTop(log_all_str)
        print "用户(出错/所有):", self.dbHelper.queryTop(user_error_str), "/", self.dbHelper.queryTop(user_all_str)
        print "链接(出错/所有):", self.dbHelper.queryTop(url_error_str), "/", self.dbHelper.queryTop(url_all_str)
        print "每个源链接出错情况(出错链接数/出错链接次数/所有链接数)："

        for site in self.sources:
            site_where = " site = '" + site + "'"
            site_url_error_distinct_str = self.addTimespanToStr("select count(distinct url) from item where " + error_where + " and " + site_where, timespan)
            site_url_error_str = self.addTimespanToStr("select count(*) from item where " + error_where + " and " +  site_where, timespan)
            site_url_all_str = self.addTimespanToStr("select count(distinct url) from item where " + site_where, timespan)

            if self.options.debug:
                print 'site_url_error_distinct_str:', site_url_error_distinct_str
                print 'site_url_error_str:', site_url_error_str
                print 'site_url_all_str:', site_url_all_str

            print site, ":", self.dbHelper.queryTop(site_url_error_distinct_str), \
                    "/", self.dbHelper.queryTop(site_url_error_str), \
                    "/", self.dbHelper.queryTop(site_url_all_str)

    def calSiteErrorDistribute(self):
        """ 计算各源错误分布 """
        target = {"name": "源", "value": "所有"}
        #conditions = self.getTimespanCondition()
        self.doAnalysisError(self.conditions, target)

        designated_site = self.options.site
        sync_site_names = (designated_site,) if designated_site else self.sources

        for sourceName in sync_site_names:
            target = {"name": sourceName, "value": "~".join(self.getTimespan())}
            site_conditions = []
            site_conditions.append(["site", "=", "'" + sourceName + "'"])
            site_conditions.extend(self.conditions)
            #conditions.extend(self.getTimespanCondition())

            if self.options.debug:
                print "source: ", sourceName, " conditions: ", site_conditions
            self.doAnalysisError(site_conditions, target)

    def doAnalysisError(self, conditions = None, target = None):
        """ 根据传入条件分析错误比例 """
        itemTotal = self.getTotalItemCount(conditions)
        errorItemTotal = self.getTotalErrorItemCount(conditions)

        rate = {"name": "比例", "value": self.percentage(errorItemTotal, itemTotal) + "(" + str(errorItemTotal) + "/" + str(itemTotal) + ")"}

        queryStr = "select code, count(*) from item"
        queryStr = self.addAndConditionsToStr(conditions, queryStr)
        queryStr_code = queryStr + " and code != 0"
        for ignore_code in self.ignore_codes:
            queryStr_code += " and code != " + ignore_code
        queryStr_code += " group by code"

        if self.options.debug:
            print 'queryStr_code:', queryStr_code

        results = self.dbHelper.query(queryStr_code)
        if results:
            errorInfos = []
            for code, errorCodeItemCount in results:
                if self.options.debug:
                    print "doAnalysisError code:", code, " count:", errorCodeItemCount
                value = self.percentage(errorCodeItemCount, errorItemTotal)
                if value and value != "0.00%":
                    errorInfos.append({"code": str(code), "value": value})
            self.show(target, rate, errorInfos)

    def show(self, target, rate, errorInfos):
        """ 以表格形式打印错误数据 """
        keys = []
        keys.append(target.get("name"))
        keys.append(rate.get("name"))

        values = []
        values.append(target.get("value"))
        values.append(rate.get("value"))

        for error_info in errorInfos:
            keys.append(error_info.get("code"))
            values.append(error_info.get("value"))

        print
        #row_format ="{:<15}" * (len(keys))
        #print row_format.format(*keys)
        #print row_format.format(*values)
        print "||"," || ".join(keys),"||"
        print "||", " || ".join(values), "||"

    def calNewErrorDistribute(self):
        """ 计算每日新增错误的分布 """
        # 计算数据库中所有日志的时间天数
        minTime = self.getEarlyTime()
        minDateTime = self.timeUtil.getDatetimeFromStr(minTime)
        now = datetime.datetime.now()
        days = now.day - minDateTime.day

        if self.options.debug:
            print "minTime:", minTime, "now:", now, "days:", days

        # 用于存放所有不重复的错误
        # (url, code) 组成一个惟一标识的错误
        items_total = {}
        total = 0

        print "时间:", "当天新增错误/当天总错误"
        for i in reversed(range(days + 1)):
            timespan = self.timeUtil.getDayFromNowSpanStr(-i)
            queryStr = "select url, code from item where code != 0 and code != 35"
            queryStr = self.addTimespanToStr(queryStr, timespan)

            if self.options.debug:
                print "calNewErrorOnDate queryStr: ", queryStr

            items = self.dbHelper.query(queryStr)
            increase_count = 0

            for item in items:
                key = item[0] + "_" + str(item[1])
                if not items_total.get(key, None):
                    increase_count += 1
                items_total[key] = items_total.get(key, 0) + 1
            total += increase_count

            print str(timespan[0].month) + "." + str(timespan[0].day) + ":", increase_count, "/", len(items)
        print "total:", total

    def getTimespan(self):
        """ 获取时间段 """
        if self.options.timespan:
            # 返回用户指定时间段
            return re.split(',~|', self.options.timespan)
        elif self.basetime:
            # 返回初始时默认时间点
            return (self.basetime,)
        else:
            # 返回数据库中最早的时间点
            return (self.getEarlyTime(),)

    def getTimespanCondition(self):
        """ 获取用户输入的时间段 """
        uponTime = self.getTimespan()
        time_conditions = []
        time_conditions.append(["uploadTime", ">", "'" + uponTime[0] + "'"])
        if len(uponTime) > 1:
            time_conditions.append(["uploadTime", "<", "'" + uponTime[1] + "'"])
        return time_conditions

    def getLuaVersionCondition(self):
        """ 获取Lua脚本版本参数 """
        lua_version = self.options.lua_version
        if lua_version:
            lua_version_condition = ["lua_version", ">", "'" + lua_version + "'"]
            return lua_version_condition

    def addAndConditionsToStr(self, conditions, queryStr):
        """ 追加查询条件到 Query 语句中 """
        if conditions and queryStr:
            for index, cond in enumerate(conditions):
                if index == 0:
                    queryStr += " where "
                else:
                    queryStr += " and "
                queryStr += " ".join(cond)

        return queryStr

    def addTimespanToStr(self, queryStr, timespan, first=False):
        """ 追加时间间隔到 Query 语句中 """
        queryStr += " where " if first else " and "
        queryStr += " uploadTime > '" + str(timespan[0]) + "'"
        if len(timespan) > 1:
            queryStr += " and uploadTime < '" + str(timespan[1]) + "'"
        return queryStr

    def percentage(self, part, total):
        """ 转换浮点数为百分数 """
        if total == 0:
            return "0.00%"
        return "{:.2%}".format(float(part)/float(total))


class DBHelper:
    '数据库处理类'

    def __init__(self):
        self.db = sqlite3.connect("lua_data.db")
        self.cursor = self.db.cursor()
        if not self.checkTableExists():
            self.create_db()

    def checkTableExists(self, table="item"):
        '检测 item 表是否创建'
        return None

    def create_db(self):
        '创建数据库'
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS item (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                site TEXT,
                code INTEGER,
                uuid TEXT,
                msg TEXT,
                url TEXT,
                version TEXT,
                lua_version INTEGER,
                uploadTime TEXT
            )
        ''')

    def store(self, item):
        '插入数据项'
        queryStr = 'insert into item(site, code, uuid, msg, url, version, lua_version, uploadTime) values (?,?,?,?,?,?,?,?)'
        vals = [item["site"], item["code"], item["uuid"], item["msg"], item["url"], item["version"], item["lua_version"], item["uploadTime"]]
        self.cursor.execute(queryStr, vals)
        self.db.commit()

    def queryTop(self, queryStr):
        '执行查询语句，返回单条结果'
        self.cursor.execute(queryStr)
        row = self.cursor.fetchone()

        if row:
            return row[0]

    def query(self, queryStr):
        '执行查询语句，返回多条结果'
        self.cursor.execute(queryStr)
        row = self.cursor.fetchall()

        return row


class TimeUtil:

    ISOTIMEFORMAT='%Y-%m-%d %X'

    def getCurrentTimeStr(self):
        """ 获取当前系统时间，以字符串形式返回 """
        return time.strftime(ISOTIMEFORMAT)

    def getDayFromNowSpanStr(self, delta = 0):
        """ 获取(当天 + delta天)的时间段, delta以天为单位 """
        now = datetime.datetime.now()
        time_start = datetime.datetime(now.year, now.month, now.day + delta)
        time_end = datetime.datetime(now.year, now.month, now.day + delta, 23, 59, 59)

        return (time_start, time_end)

    def getDatetimeFromStr(self, s):
        """ 转化字符串为 datetime 对象 """
        format = '%Y-%m-%d %H:%M:%S'
        return datetime.datetime.strptime(s, format)


def main():
    """ 解析命令行参数 """
    from optparse import OptionParser

    usage = "usage: %prog action[sync|analysis|error|sql] [options]"
    parser = OptionParser(usage = usage)
    parser.add_option("-d", "--debug", dest="debug", help="[Global]. Print debug messages to stdout", default=False, action="store_true")
    parser.add_option("-m", "--mode", dest="mode", help="[Analysis]. Analysis mode. Options: [site|user|url]", default="site")
    parser.add_option("-s", "--site", dest="site", help="[Sync, Analysis, Error]. Designated site", metavar="letv")
    parser.add_option("-u", "--uuid", dest="uuid", help="[Analysis]. Designated user")
    parser.add_option("-r", "--url", dest="url", help="[Analysis]. Designated url")
    parser.add_option("-c", "--code", dest="code", help="[Analysis]. Designated code")
    parser.add_option("-l", "--lua", dest="lua_version", help="[Analysis, Error]. Designated lua_version")
    parser.add_option("-q", "--sql", dest="sql", help="[Sql]. Raw sql sentence")
    parser.add_option("-t", "--timespan", dest="timespan", help="[Analysis, Error]. Timespan for analysis")
    parser.add_option("-p", "--side", dest="side", default="site", help="[Error]. Side for error analysis. Options: [all|site|increment]")

    options, args = parser.parse_args()
    if len(args) != 1:
        parser.error("incorrect number of arguments")

    now = datetime.datetime.now()
    startTime = datetime.datetime(now.year, now.month, now.day -1, 19, 0, 0)

    analysis = Analysis(options, args, str(startTime))
    analysis.startWork()


if __name__ == '__main__':
    main()
