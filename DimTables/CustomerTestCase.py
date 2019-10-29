# -*- coding: UTF-8 -*-
import unittest
from abc import abstractmethod
from impala.dbapi import connect
from impala.util import as_pandas
import sys
import TableTestCase
import datetime

reload(sys)
sys.setdefaultencoding('utf-8')


class CustomerTestCase(TableTestCase.TableTestCase):
    """顾客维度表测试"""
    @classmethod
    def setUpClass(self):
        self.tbName = "db.dim_customer"
        super(CustomerTestCase, self).setUpClass()

    @unittest.skip(u"暂停")
    def test_RowCount(self):
        pass

    def test_MessyCode(self):
        cs = self.icon.cursor()
        try:
            sql = """ select customer_name from db.dim_customer 
                    where substr(create_time,1,10) like (select substr(max(member_create_time),1,10) 
                    from db.dim_customer) and customer_type like '%member%' and customer_name <>'' limit 1000 """
            cs.execute(sql)
            df = as_pandas(cs)
            ldf = float(len(df))
            rs = self.findmessycode(df)
            if len(rs)/ldf > 0.2:  # 容忍度
                self.raiseFailure("执行查询：'{0}'. 发现乱码：{1}".format(sql, ",".join(rs[0:15])))
            elif len(rs)/ldf > 0.1:  # 容忍度
                self.raiseError("执行查询：'{0}'. 发现疑似乱码：{1}".format(sql, ",".join(rs)))
        finally:
            cs.close()

    def test_LatestDate(self):
        cs = self.icon.cursor()
        try:
            cs.execute(" select max(member_create_time) as ct from db.dim_customer ")
            df = as_pandas(cs)
            rs = str(df["ct"][0])
            dt = datetime.datetime.strptime(rs, "%Y-%m-%d %H:%M:%S")
            num = (datetime.datetime.now() - dt).days
            if num in range(1, 8):
                self.raiseError(u"数据很久没更新了，上一次数据更新在： " + rs)
            if num >= 8:
                self.raiseFailure(u"数据很久没更新了，上一次数据更新在：" + rs)
        finally:
            cs.close()

    @unittest.skip(u"该表不适用此测试项")
    def test_DateTimeSpanTest(self):
        pass

    @unittest.skip(u"该表不适用此测试项")
    def test_DateConsecutiveness(self):
        pass

    def test_zBizRule1(self):
        cs = self.icon.cursor()
        try:
            cs.execute(""" select count(*) ct ,substr(c.member_create_time,1,10) dt from db.dim_customer as c
                            join (select substr(max(member_create_time),1,10) mc from db.dim_customer) sub 
                            on substr(c.member_create_time,1,10)=sub.mc
                            group by substr(c.member_create_time,1,10) """)
            df = as_pandas(cs)
            rs = int(df["ct"][0])
            dt = str(df["dt"][0])
            if rs not in range(5000, 100000):
                self.raiseFailure(u"%s新增会员数不在5000和100000之间" % dt)
        finally:
            cs.close()