#!/usr/bin/env python
# encoding: utf-8

import httplib2
import json
import sqlite3

# TODO: 实现命令行参数
# TODO: 根据时间、用户、源等多个维度进行分析
# TODO: 绘制图表
# TODO: 定时同步数据
# TODO: 数据存储优化

class Analysis:

    '''
    请求源的名称
    '''
    sources = [
        u'tudou', u'wole56', u'sina', u'sohu',
        u'youku', u'cntv', u'm1905', u'letv',
        u'qiyi', u'qq', u'fengxing', u'pps',
        u'bps', u'pptv', u'kankan', u'tv189',
        u'baofeng'
    ]

    '''
    请求源的日志地址的链接前缀
    '''
    source_prefix = u"http://api.wanhuatong.tv/lua/geterror?site="

    '''
    当前日志中所有用户
    '''
    user_list = []

    '''
    当前所有日志的总数
    '''
    log_total_count = 0

    '''
    所有错误码
    '''
    error_codes = [
        "1", "2", "6", "32", "33", "34", "35", "37"
    ]

    '''
    所有错误码的描述
    '''
    error_desc = {
        "1":"get page failed",
        "2":"parse page failed",
        "6":"video not exists",
        "32":"script run crash",
        "33":"can not found lua script",
        "34":"crawl result invalid",
        "35":"script time out",
        "37":"script decrypt error"
    }

    def __init__(self):
        self.dbHelper = DBHelper()

    def syncData(self):
        '同步Lua抓取的后台日志'

        lastTime = self.dbHelper.getLastTime()
        if lastTime:
            print "lastTime: ", lastTime

        h = httplib2.Http(".cache")
        for sourceName in self.sources:
            print "start source: ", sourceName

            resp, content = h.request(self.source_prefix + sourceName)
            if not content:
                print "source ", sourceName, "return null"
                continue

            try:
                msgObj = json.loads(content)
                msgCode = msgObj.get("code")
                if msgCode == 0:
                    msgMsg = msgObj.get("msg")
                    for itemObj in msgMsg:
                        self.storeItem(lastTime, itemObj)
                        user = itemObj.get("uuid")
                        if user not in self.user_list:
                            self.user_list.append(user)
                    self.log_total_count += len(msgMsg)
                    print sourceName, "current has log:", len(msgMsg)
            except Exception, ex:
                print "source: ", sourceName, ex.message

        print "current len of user: ", len(self.user_list)
        print "currnet total log count: ", self.log_total_count

    def analy(self, sourceCondition = None):
        '分析错误比例'
        queryTotalStr = "select count(*) from item where code != 0"
        if sourceCondition:
            queryTotalStr += " and site = '" + sourceCondition + "'"

        errorTotal = self.dbHelper.query(queryTotalStr)
        errorCounts = {}
        for code in self.error_codes:
            queryCodeStr = "select count(*) from item where code = " + code
            if sourceCondition:
                queryCodeStr += " and site = '" + sourceCondition + "'"
            errorCount = self.dbHelper.query(queryCodeStr)
            key = code + "(" + self.error_desc[code] + ")"
            errorCounts[key] = self.percentage(errorCount, errorTotal)
        print errorCounts

    def analySource(self):
        '分析各个源的错误比例'
        for sourceName in self.sources:
            print "start analysis source: ", sourceName
            self.analy(sourceCondition = sourceName)

    def analyTotal(self):
        '分析所有错误的比例分布'
        print "start analysis total"
        self.analy()

    def doAnalysis(self):
        '对错误进行比例分析'
        self.analyTotal()
        self.analySource()

    def percentage(self, part, total):
        '转换浮点数为百分数'
        if total == 0:
            return "0.00%"
        return "{:.2%}".format(float(part)/float(total))

    def parseErrorData(self, data):
        '从Json字符串解析数据'
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
        '存储日志'
        item = self.parseErrorData(itemObj)
        if item["uploadTime"] > lastTime:
            print "Store new item: ", item
            self.dbHelper.store(item)


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
        query = 'insert into item(site, code, uuid, msg, url, version, lua_version, uploadTime) values (?,?,?,?,?,?,?,?)'
        vals = [item["site"], item["code"], item["uuid"], item["msg"], item["url"], item["version"], item["lua_version"], item["uploadTime"]]
        self.cursor.execute(query, vals)
        self.db.commit()

    def query(self, queryStr):
        '执行查询语句'
        self.cursor.execute(queryStr)
        row = self.cursor.fetchone()

        if row:
            return row[0]

    def getLastTime(self):
        '获取最近一条日志的时间'
        query = 'select max(uploadTime) from item'
        self.cursor.execute(query)
        row = self.cursor.fetchone()

        if row:
            return row[0]


if __name__ == "__main__":
    analysis = Analysis()
    analysis.syncData()
    #analysis.doAnalysis()
