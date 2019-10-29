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


class TotalOrderTransTestCase(TableTestCase.TableTestCase):
    @classmethod
    def setUpClass(self):
        self.tbName = "db.fct_total_order_trans"
        super(TotalOrderTransTestCase, self).setUpClass()

    @unittest.skip("暂停")
    def test_RowCount(self):
        pass

    @unittest.skip("该表不适用此测试项,没有中文")
    def test_MessyCode(self):
        pass

    def test_LatestDate(self):
        cs = self.icon.cursor()
        try:
            cs.execute("select max(order_date) as maxd from db.fct_total_order_trans")
            df = as_pandas(cs)
            rs = str(df["maxd"][0])
            dt = datetime.datetime.strptime(rs, "%Y-%m-%d")
            num = (datetime.datetime.now() - dt).days
            if num > 1 and num < 8:
                self.raiseError("数据很久没更新了，上一次数据更新在： " + rs)
            if num >= 8:
                self.raiseFailure("数据很久没更新了，上一次数据更新在：" + rs)
        finally:
            cs.close()

    @unittest.skip("暂停")
    def test_DateTimeSpanTest(self):
        pass

    def test_DateConsecutiveness(self):
        cs = self.icon.cursor()
        try:
            cs.execute("select distinct trim(order_date) order_date from db.fct_total_order_trans")
            df = as_pandas(cs)
            rs = df["order_date"].tolist()
            breakdates = TableTestCase.TableTestCase.ConsecutivenessDateTest(rs, "%Y-%m-%d")
            if len(breakdates) > 0:
                self.raiseFailure("全渠道交易表数据存在中断，在%s和%s之间 " % (breakdates[0], breakdates[1]))
        finally:
            cs.close()

    def test_zBizRule1(self):
        cs = self.icon.cursor()
        try:
            cs.execute(""" select s.store_name, a.ct as store_order_count from (
                    select count(1) ct,store_key from db.fct_total_order_trans 
                    where order_date_key=(select max(order_date_key) from db.fct_total_order_trans) and 
                    data_source='store' 
                    group by store_key 
                    having count(1)<50
                    ) a join db.dim_store s on s.store_key = a.store_key and s.latest_date_flag=1 """)
            df = as_pandas(cs)
            rs = 0 if df is None else len(df)
            if rs > 10:
                self.raiseFailure("全渠道交易表：存在大量销售量小于50单的店铺。<br /> %s" % df.to_html())
            if rs in range(1, 11) :
                self.raiseError("全渠道交易表：存在少量销售量小于50的店铺。<br /> %s" % df.to_html())
        finally:
            cs.close()

    def test_zBizRule2(self):
        cs = self.icon.cursor()
        try:
            cs.execute(""" select max(order_date) as maxd from db.fct_total_order_trans """)
            df = as_pandas(cs)
            maxd = str(df["maxd"][0])
            lastyeard = TableTestCase.TableTestCase.GetLastYearSameDay(maxd)
            sql = """select a.store_key,a.t_amt/b.t_amt from 
                (select sum(real_amt) t_amt,store_key 
                from db.fct_total_order_trans where order_date='%s' and data_source='store' group by store_key) a 
                join 
                (select sum(real_amt) t_amt,store_key from db.fct_total_order_trans where order_date='%s' group by 
                store_key) b on a.store_key=b.store_key 
                where a.t_amt/b.t_amt is not null and (a.t_amt/b.t_amt>3 or a.t_amt/b.t_amt <0.3)""" % (maxd, lastyeard)
            cs.execute(sql)
            df = as_pandas(cs)
            rs = len(df)
            if rs > 20:
                self.raiseFailure("全渠道交易表：去年同日的销售量对比，发现差异过大的数据。<br />%s" % df.to_html())
            if rs in range(1, 21):
                self.raiseError("全渠道交易表：去年同日的销售量对比，发现差异过大的数据。<br />%s" % df.to_html())
        finally:
            cs.close()

    def test_zBizRule3(self):
        """涨价商品 original_price<current_price"""
        cs = self.icon.cursor()
        try:
            cs.execute(""" select count(*) ct from fct_total_order_trans where original_price<current_price and 
                        order_date_key=(select max(order_date_key) from db.fct_total_order_trans) """)
            df = as_pandas(cs)
            ct = int(df["ct"][0])
            if ct > 0:
                self.raiseFailure("全渠道交易表：original_price<current_price")
        finally:
            cs.close()

    def test_zBizRule4(self):
        cs = self.icon.cursor()
        try:
            cs.execute(""" select data_source,count(1) from db.fct_total_order_trans where order_date_key=
                        (select max(order_date_key) from db.fct_total_order_trans) and 
                        data_source in ("tmall","bs") group by data_source having count(1)<100 """)
            df = as_pandas(cs)
            ct = len(df)
            if ct > 0:
                self.raiseFailure("全渠道交易表：TMall&官网 日别交易量不少于100单")
        finally:
            cs.close()

    def test_zBizRule5(self):
        cs = self.icon.cursor()
        try:
            cs.execute(""" select max(order_date) as maxd from db.fct_total_order_trans """)
            df = as_pandas(cs)
            maxd = str(df["maxd"][0])
            lastyeard = TableTestCase.TableTestCase.GetLastYearSameDay(maxd)
            sql = """ select a.data_source,a.total_amt/b.total_amt from 
                     (select sum(total_amt) total_amt,data_source from db.fct_total_order_trans where order_date='%s' 
                     and data_source in ('bs','tmall') group by data_source) a join 
                     (select sum(total_amt) total_amt,data_source from db.fct_total_order_trans where order_date='%s' 
                     and data_source in ('bs','tmall') group by data_source) b
                     on a.data_source=b.data_source where a.total_amt/b.total_amt is not null 
                     and (a.total_amt/b.total_amt>3 or a.total_amt/b.total_amt <0.3) """ % (maxd, lastyeard)
            cs.execute(sql)
            df = as_pandas(cs)
            ct = len(df)
            if ct > 0:
                self.raiseFailure("全渠道交易表：TMall&官网 日别交易量不少于100单")
        finally:
            cs.close()

    def test_zBizRule6(self):
        cs = self.icon.cursor()
        try:
            cs.execute(""" select distinct order_date from db.fct_total_order_trans where data_source = 'bs' 
                           and order_date >='2018-09-01'""")
            df = as_pandas(cs)
            rs = df["order_date"].tolist()
            breakdates = TableTestCase.TableTestCase.ConsecutivenessDateTest(rs, "%Y-%m-%d")
            if len(breakdates) > 0:
                self.raiseFailure("全渠道交易表官网数据存在中断，在%s和%s之间 " % (breakdates[0], breakdates[1]))

            cs.execute("""select distinct order_date from db.fct_total_order_trans where data_source = 'tmall'""")
            df = as_pandas(cs)
            rs = df["order_date"].tolist()
            breakdates = TableTestCase.TableTestCase.ConsecutivenessDateTest(rs, "%Y-%m-%d")
            if len(breakdates) > 0:
                self.raiseFailure("全渠道交易表Tmall数据存在中断，在%s和%s之间 " % (breakdates[0], breakdates[1]))

            cs.execute("""select distinct order_date from db.fct_total_order_trans where data_source like '%o2o%'
                          and order_date >='2016-09-01'""")
            df = as_pandas(cs)
            rs = df["order_date"].tolist()
            breakdates = TableTestCase.TableTestCase.ConsecutivenessDateTest(rs, "%Y-%m-%d")
            if len(breakdates) == 0:
                pass
            # Tmall switch on 2018/5/16
            elif len(breakdates) in (2, 3) and str(breakdates[0]).strip() == '2018-05-15 00:00:00' \
                    and str(breakdates[1]).strip() == '2018-05-17 00:00:00':
                pass
            else:
                self.raiseFailure("全渠道交易表O2O数据存在中断，在 %s " % ','.join([str(s) for s in breakdates]))
        finally:
            cs.close()

    def test_zBizRule7(self):
        cs = self.icon.cursor()
        try:
            cs.execute("""select count(*) ct from db.fct_total_order_trans where order_type_key=3 and qty>0""")
            df = as_pandas(cs)
            ct = int(df["ct"][0])
            if ct > 40:
                self.raiseFailure("存在退款数据的qty>0")
            if ct in range(1, 41):
                self.raiseError("存在退款数据的qty>0")
        finally:
            cs.close()

    def test_zBizRule8(self):
        """客单价对比"""
        cs = self.icon.cursor()
        try:
            cs.execute(""" select max(order_date) as maxd from db.fct_total_order_trans """)
            df = as_pandas(cs)
            maxd = str(df["maxd"][0])
            lastyeard = TableTestCase.TableTestCase.GetLastYearSameDay(maxd)
            sql = """select a.store_key,a.avg_amt/b.avg_amt from 
                    (select sum(total_amt)/count(distinct order_no) avg_amt,store_key from db.fct_total_order_trans 
                    where data_source='store' and order_date='%s' group by store_key) a join 
                    (select sum(total_amt)/count(distinct order_no) avg_amt,store_key from db.fct_total_order_trans 
                    where data_source='store' and order_date='%s' group by store_key) b on a.store_key = b.store_key
                    where a.avg_amt/b.avg_amt>1.5 or a.avg_amt/b.avg_amt<0.5 """ % (maxd, lastyeard)
            cs.execute(sql)
            df = as_pandas(cs)
            if len(df) > 0:
                self.raiseError("存在不满足去年同日销售预期比例的数据。")
        finally:
            cs.close()

    def test_zBizRule9(self):
        cs = self.icon.cursor()
        try:
            cs.execute(""" select count(*) ct from db.fct_total_order_trans where customer_key is null """)
            df = as_pandas(cs)
            ct = int(df["ct"][0])
            if ct > 0:
                self.raiseFailure("全渠道交易表：存在空的customer_key")
        finally:
            cs.close()

    def test_epaydataontime(self):
        cs = self.icon.cursor()
        try:
            dby = (datetime.date.today() + datetime.timedelta(days=-2)).strftime("%Y%m%d")
            cs.execute(""" select s.store_id, s.store_name,a.d max_date,a.src source from (
                select store_key,max(order_date_key) d,data_source src  from db.fct_total_order_trans
                where data_source  in ('store')
                group by store_key,data_source
                having max(order_date_key)< %s ) a 
                join db.dim_store s on s.store_key=a.store_key and s.latest_date_flag=1
                order by a.d desc """ % dby)
            df = as_pandas(cs)
            ct = len(df)
            if ct > 0 and ct <= 20:
                self.raiseError("全渠道交易表：门店epay渠道的数据更新延迟。<br /> %s" % df.to_html())
            elif ct > 20:
                self.raiseFailure("全渠道交易表：门店epay渠道的数据更新延迟。<br /> %s" % df.to_html())
        finally:
            cs.close()


