#!/usr/bin/env python2
# encoding: utf-8

import httplib2
import json
import sqlite3

# TODO: 实现命令行参数
# TODO: 根据时间、用户、源等多个维度进行分析
# TODO: 绘制图表
# TODO: 定时同步数据
# TODO: 数据存储优化
# TODO: 网盘源特殊分析处理

class Analysis:

    """请求源的名称"""
    sources = [
        u'tudou', u'wole56', u'sina', u'sohu',
        u'youku', u'cntv', u'm1905', u'letv',
        u'qiyi', u'qq', u'fengxing', u'pps',
        u'bps', u'pptv', u'kankan', u'tv189',
        u'baofeng'
    ]

    """请求源的日志地址的链接前缀"""
    source_prefix = u"http://api.wanhuatong.tv/lua/geterror?site="

    """当前日志中所有用户"""
    user_list = []

    """使用论坛测试包的用户"""
    bbs_user_list = []

    """当前所有日志的总数"""
    log_total_count = 0

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

    """ 该次数据同步添加的日志数 """
    appendItemCount = 0

    """数据同步错误日志"""
    log_file_name = "sync_log"

    """ 是否Debug """
    _debug = False;

    def __init__(self):
        self.dbHelper = DBHelper()

    def syncData(self):
        """ 同步Lua抓取的后台日志 """

        lastTime = self.getLastTime()
        print "lastTime: ", lastTime

        failed_sites = []

        h = httplib2.Http(".cache")
        for sourceName in self.sources:
            print "start source: ", sourceName
            resp, content = h.request(self.source_prefix + sourceName)

            retry = 0
            while not content and retry > 2: # 网络请求重试
                print "Failed ", sourceName, " Retry"
                resp, content = h.request(self.source_prefix + sourceName)
                retry += 1

            if not content:
                failed_sites.append(sourceName)
                continue

            # 存储数据
            self.parseData(sourceName, content, lastTime)

        print "Finish Sync Data."
        print "Append ", self.appendItemCount, " Items. Total Items Count: ", self.getTotalItemCount()
        print "Total User Count: ", self.getUserCount(), " New BBS User Count: ", self.getBBSUserCount()
        print "Failed Sites: ", failed_sites

        if failed_sites:
            log_file = open(self.log_file_name, 'a')
            log_file.write("Error: " + ",".join(failed_sites))
            log_file.close()

    def parseData(self, sourceName, content, lastTime):
        """ 数据解析、存储 """
        try:
            msgObj = json.loads(content)
            msgCode = msgObj.get("code")
            if msgCode == 0:
                msgMsg = msgObj.get("msg")
                for itemObj in msgMsg:
                    self.storeItem(lastTime, itemObj)
                print sourceName, "current has log:", len(msgMsg)
        except Exception, ex:
            print "source: ", sourceName, ex.message

    def parseErrorData(self, data):
        """ 从Json字符串解析数据 """
        Item = {}
        Item["site"] = data.get("site", "")
        Item["code"] = data.get("code", 0)
        Item["uuid"] = data.get("uuid", "")
        Item["msg"]  = data.get("msg", "")
        Item["url"]  = data.get("url", "")
        Item["version"] = data.get("version", "")
        Item["lua_version"] = data.get("lua_version", 0)
        Item["uploadTime"] = data.get("uploadTime", "")

        return Item

    def storeItem(self, lastTime, itemObj):
        """ 存储日志 """
        item = self.parseErrorData(itemObj)
        if item["uploadTime"] > lastTime:
            print "Store new item: ", item
            self.dbHelper.store(item)
            self.appendItemCount += 1

    def doAnalysis(self, conditions = None, target = None):
        """ 根据传入条件分析错误比例 """
        itemTotal = self.getTotalItemCount(conditions)
        errorItemTotal = self.getTotalErrorItemCount(conditions)

        rate = {"name": "比例", "value": self.percentage(errorItemTotal, itemTotal) + "(" + str(errorItemTotal) + "/" + str(itemTotal) + ")"}

        queryStr = "select count(*) from item"
        for index, cond in enumerate(conditions):
            if index == 0:
                queryStr += " where "
            else:
                queryStr += " and "

            queryStr += " ".join(cond)

        errorInfos = []

        for code in self.error_codes:
            queryStr_code = queryStr + " and code = " + code
            if self._debug:
                print 'queryStr_code: ', queryStr_code
            errorCodeItemCount = self.dbHelper.queryTop(queryStr_code)

            value = self.percentage(errorCodeItemCount, errorItemTotal)
            if value and value != "0.00%":
                errorInfos.append({"code": code, "value": value})

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

        row_format ="{:<15}" * (len(keys))
        print
        #print row_format.format(*keys)
        #print row_format.format(*values)
        print "||"," || ".join(keys),"||"
        print "||", " || ".join(values), "||"

    def analysis(self, dimension = None, uponTime = None, *params):
        """ 针对维度及时间段进行日志分析 """
        print "analysis dimension: ", dimension, " uponTime: ", uponTime

        dimension = dimension or "site"
        if not uponTime or uponTime == "0":
            uponTime = "2014-03-16 19"

        if dimension == 'site':
            self.analysisOnSite((uponTime,), *params)
        elif dimension == 'user':
            self.analysisOnUser((uponTime,), *params)
        elif dimension == 'media':
            self.analysisOnMedia((uponTime,), *params)
        elif dimension == 'script':
            self.analysisOnScript((uponTime,), *params)
        else:
            print "You suck."

    def analysisOnSite(self, uponTime, *params):
        """ 针对 源、时间段 进行日志分析 """
        for sourceName in self.sources:
            target = {"name": "源", "value": sourceName}
            conditions = []
            conditions.append(["site", "=", "'" + sourceName + "'"])
            conditions.append(["uploadTime", ">", "'" + uponTime[0] + "'"])
            if len(uponTime) > 1:
                conditions.append(["uploadTime", "<", "'" + uponTime[1] + "'"])

            print "source: ", sourceName, " conditions: ", conditions
            self.doAnalysis(conditions, target)

    def analysisOnUser(self, uponTime, *params):
        """ 针对 用户、时间段 进行日志分析 """
        pass

    def analysisOnMedia(self, uponTime, *params):
        """ 针对 影片、时间段 进行日志分析 """
        if params:
            media = params[0]
            self.analysisOnSpecMedia(uponTime, media)
        else:
            self.analysisAllErroredMedia(uponTime)

    def analysisOnScript(self, uponTime, *params):
        """ 针对 Lua脚本、时间段 进行日志分析 """
        pass

    def analysisOnSpecMedia(self, uponTime, media):
        """ 分析指定影片的错误日志 """

        target = {"name": "影片", "value": media}

        conditions = []
        con1 = ["url", "like", "'%" + media + "%'"]

        try:
            media.index("//")
            media1 = media.replace("//", "\/\/")
            media1 = media.replace("/", "\/")
        except:
            media.index("\/\/")
            media1 = media.replace("\/\/", "//")
            media1 = media.replace("\/", "/")

        if self._debug:
            print "media: ", media, " after replace media1: ", media1

        con1 = "(url like '%" + media + "%'"
        con2 = "url like '%" + media1 + "%')"

        conditions.append([con1, "or", con2])
        conditions.append(["uploadTime", ">", "'" + uponTime[0] + "'"])
        if len(uponTime) > 1:
            conditions.append(["uploadTime", "<", "'" + uponTime[1] + "'"])

        if self._debug:
            print "media: ", media, " conditions: ", conditions
        self.doAnalysis(conditions, target)

    def analysisAllErroredMedia(self, uponTime):
        """ 分析所有曾经出现错误的影片的错误日志 """
        queryStr = "select url from item where code != 0 and site = 'pptv'"
        for ignore_code in self.ignore_codes:
            queryStr += " and code != " + ignore_code
        queryStr += " and uploadTime" + ">" + "'" + uponTime[0] + "'"
        if len(uponTime) > 1:
            queryStr += " and uploadTime" + "<" + "'" + uponTime[1] + "'"
        queryStr += " group by url"

        erroredUrls = self.dbHelper.query(queryStr)
        if erroredUrls:
            if self._debug:
                print "We got ", len(erroredUrls), " errored urls"
            for error_url in erroredUrls:
                self.analysisOnSpecMedia(uponTime, error_url[0])

    def percentage(self, part, total):
        '转换浮点数为百分数'
        if total == 0:
            return "0.00%"
        return "{:.2%}".format(float(part)/float(total))

    def getBBSUserCount(self):
        queryStr = "select count(*) from item where lua_version != 0 group by uuid"
        uuid_counts = self.dbHelper.query(queryStr)

        if uuid_counts:
            return len(uuid_counts)

    def getUserCount(self):
        queryStr = "select count(*) from item group by uuid"
        uuid_counts = self.dbHelper.query(queryStr)

        if uuid_counts:
            return len(uuid_counts)

    def getTotalItemCount(self, conditions = None):
        """ 计算日志的总数 """
        queryStr = "select count(*) from item"

        if conditions:
            for index, cond in enumerate(conditions):
                if index == 0:
                    queryStr += " where "
                else:
                    queryStr += " and "
                queryStr += " ".join(cond)

        if self._debug:
            print 'queryStr_total: ', queryStr
        return self.dbHelper.queryTop(queryStr)

    def getTotalErrorItemCount(self, conditions = None):
        """ 计算错误日志的总数 """
        quertStr_total_error = "select count(*) from item where code != 0";

        for ignore_code in self.ignore_codes:
            quertStr_total_error += " and code != " + ignore_code
        if conditions:
            for cond in conditions:
                quertStr_total_error += " and " + " ".join(cond)

        if self._debug:
            print 'queryStr_total_error: ', quertStr_total_error
        return self.dbHelper.queryTop(quertStr_total_error)

    def getLastTime(self):
        """ 获取最近一条日志的时间 """
        queryStr = "select max(uploadTime) from item"
        return self.dbHelper.queryTop(queryStr)


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


def main(argv):

    import getopt

    try:
        opts, args = getopt.getopt(argv, "saubd")
    except getopt.GetoptError:
        sys.exit(2)

    analysis = Analysis()

    for opt, arg in opts:
        if opt == '-s':
            print "Try to sync data"
            analysis.syncData()
        elif opt == '-a':
            print "Try to analysis data"
            analysis.analysis(*args)
        elif opt == '-u':
            print "Try to cal user"
            analysis.getUserCount()
        elif opt == '-b':
            print "Try to cal bbs user"
            analysis.getBBSUserCount()
        elif opt == '-d':
            print "Set debug mode"
            analysis._debug = True
        else:
            print "You suck"

if __name__ == "__main__":
    import sys
    main(sys.argv[1:])
