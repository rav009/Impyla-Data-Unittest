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


class AssortMasterTestCase(TableTestCase.TableTestCase):
    @classmethod
    def setUpClass(self):
        self.tbName = "db.rel_assort_master"
        super(AssortMasterTestCase, self).setUpClass()

    @unittest.skip(u"暂停")
    def test_RowCount(self):
        cs = self.icon.cursor()
        try:
            cs.execute("select count(*) as ct from db.rel_assort_master")
            df = as_pandas(cs)
            rs = int(df["ct"][0])
            if rs == 0:
                self.raiseFailure(u"数据行数失败：空表" )
            if not (rs < 750000 and rs > 2000000):
                self.raiseError("数据行数测试异常: " + str(rs))
        finally:
            cs.close()

    @unittest.skip(u"该表不适用此测试项,没有中文")
    def test_MessyCode(self):
        pass

    def test_LatestDate(self):
        cs = self.icon.cursor()
        try:
            cs.execute("select max(create_time) as ct from db.rel_assort_master")
            df = as_pandas(cs)
            rs = str(df["ct"][0])
            dt = datetime.datetime.strptime(rs, "%Y-%m-%d %H:%M:%S")
            num = (datetime.datetime.now() - dt).days
            if num > 1 and num < 8:
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
            cs.execute("select max(p.year) maxyr,min(p.year) minyr from db.rel_assort_master a join "
                       "db.dim_product p on cast(a.product_key as int)=p.product_key")
            df = as_pandas(cs)
            maxyr = int(df["maxyr"][0])
            minyr = int(df["minyr"][0])
            rs = maxyr - minyr
            if rs < 9 or rs > 100:
                self.raiseFailure(u"db.rel_assort_master的数据存在错误，产品年份跨度为：%d" % rs)
        finally:
            cs.close()