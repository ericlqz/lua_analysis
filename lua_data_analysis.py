#!/bin/python2
# -*- coding:utf-8 -*-

import httplib2
import json

class Analysis:

    '''
    请求源的日志地址
    '''
    source_urls = [
        u'tudou', u'wole56', u'sina', u'sohu',
        u'youku', u'cntv', u'm1905', u'letv',
        u'qiyi', u'qq', u'fengxing', u'pps',
        u'bps', u'pptv', u'kankan', u'tv189',
        u'baofeng'
    ]

    source_prefix = u"http://api.wanhuatong.tv/lua/geterror?site="

    user_list = []

    def getUserCount(self):
        '''
        获取使用前端抓取的用户总数
        '''
        h = httplib2.Http(".cache")
        for sourceName in self.source_urls:
            resp, content = h.request(self.source_prefix + sourceName)
            msgObj = json.loads(content)
            msgCode = msgObj.get("code")
            if msgCode == 0:
                msgMsg = msgObj.get("msg")
                for item in msgMsg:
                    user = item.get("uuid")
                    if user not in self.user_list:
                        self.user_list.append(user)
                print sourceName, "has log:", len(msgMsg)
                print "len of user: ", len(self.user_list)

    def getCrawlCount(self):
        '''
        获取当前前端抓取总抓取数
        '''
        pass

    def getSourceErrorDistribute(self):
        '''
        获取每个源中错误分布比例
        '''
        pass

if __name__ == "__main__":
    analysis = Analysis()
    analysis.getUserCount()
