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


class AgWeeklySLSPlanTestCase(TableTestCase.TableTestCase):
    """门店别周别sum别销售计划表测试"""
    @classmethod
    def setUpClass(self):
        self.tbName = "db.fct_fr_ag_wk_sls_plan"
        super(AgWeeklySLSPlanTestCase, self).setUpClass()

    @unittest.skip("暂停")
    def test_RowCount(self):
        cs = self.icon.cursor()
        try:
            cs.execute("select count(*) as ct from db.fct_fr_ag_wk_sls_plan")
            df = as_pandas(cs)
            rs = int(df["ct"][0])
            if rs == 0:
                self.raiseFailure("数据行数失败：空表" )
            if not (rs < 750000 and rs > 2000000):
                self.raiseError("数据行数测试异常: " + str(rs))
        finally:
            cs.close()

    @unittest.skip("该表不适用此测试项,没有中文")
    def test_MessyCode(self):
        pass

    def test_LatestDate(self):
        cs = self.icon.cursor()
        try:
            cs.execute("select max(create_time) as ct from db.fct_fr_ag_wk_sls_plan")
            df = as_pandas(cs)
            rs = str(df["ct"][0])
            dt = datetime.datetime.strptime(rs, "%Y-%m-%d %H:%M:%S")
            num = (datetime.datetime.now() - dt).days
            if num >= 8:
                self.raiseError("数据很久没更新了，上一次数据更新在： " + rs)
            elif num >= 15:
                self.raiseFailure("数据很久没更新了，上一次数据更新在：" + rs)
        finally:
            cs.close()

    @unittest.skip("暂停")
    def test_DateTimeSpanTest(self):
        cs = self.icon.cursor()
        try:
            cs.execute(" select max(target_week_no) as maxwk, min(target_week_no) as minwk "
                       " from db.fct_fr_ag_wk_sls_plan ")
            df = as_pandas(cs)
            maxwk = int(df["maxwk"][0])
            minwk = int(df["minwk"][0])
            rs = maxwk - minwk
            if rs < 12:
                self.raiseFailure("数据历史跨度不够。"                    )
        finally:
            cs.close()

    @unittest.skip(u"该表不适用此测试项")
    def test_DateConsecutiveness(self):
        pass

    def test_zBizRule1(self):
        """ lower < upper """
        cs = self.icon.cursor()
        try:
            sql = "select count(*) as ct from db.fct_fr_ag_wk_sls_plan " \
                  "where lower_limit_sales_plan>upper_limit_sales_plan "
            cs.execute(sql)
            df = as_pandas(cs)
            rs = int(df["ct"][0])
            if rs != 0:
                self.raiseFailure("业务逻辑测试失败：%s" % sql)
        finally:
            cs.close()

    def test_zBizRule2(self):
        """ verion max/min """
        cs = self.icon.cursor()
        try:
            sql = """select max(store_setting_sales_plan)/min(store_setting_sales_plan), a.target_week_no from (
                     select target_week_no, ver_cd, sum(store_setting_sales_plan) as store_setting_sales_plan from 
                     db.fct_fr_ag_wk_sls_plan group by target_week_no, ver_cd
                     ) a group by a.target_week_no
                     having max(store_setting_sales_plan)/min(store_setting_sales_plan)>=1.5 or 
                     max(store_setting_sales_plan)/min(store_setting_sales_plan)<1"""
            cs.execute(sql)
            df = as_pandas(cs)
            rs = len(df)
            if rs in range(1, 10):
                self.raiseError("业务逻辑测试失败：同一周不通版本plan的差异过大，疑似数据质量问题。")
            elif rs >= 10:
                self.raiseFailure("业务逻辑测试失败：大量plan存在同一周不通版本的差异过大的问题。")
        finally:
            cs.close()