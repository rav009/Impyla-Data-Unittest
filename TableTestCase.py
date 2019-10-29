# -*- coding: UTF-8 -*-
import unittest
from abc import abstractmethod
from impala.dbapi import connect
from impala.util import as_pandas
import sys
import warnings
from unittest import SkipTest


reload(sys)
sys.setdefaultencoding('utf-8')


class SkipTest(Exception):
    """
    Raise this exception in a test to skip it.

    Usually you can use TestCase.skipTest() or one of the skipping decorators
    instead of raising this directly.
    """
    pass


class _ExpectedFailure(Exception):
    """
    Raise this when a test is expected to fail.

    This is an implementation detail.
    """

    def __init__(self, exc_info):
        super(_ExpectedFailure, self).__init__()
        self.exc_info = exc_info


class _UnexpectedSuccess(Exception):
    """
    The test was supposed to fail, but it didn't!
    """
    pass


class TableTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.icon = connect(host='slave.f-pro.cn', port=21050, user='u',
                            auth_mechanism='GSSAPI', password='p', database='db')
        cs = self.icon.cursor()
        try:
            cs.execute("INVALIDATE METADATA %s" % self.tbName)
        finally:
            cs.close()

    @classmethod
    def tearDownClass(self):
        self.icon.close()

    @abstractmethod
    def test_RowCount(self):
        """主要是验收时候用，看项目上线时的历史数据量是否充足"""
        raise NotImplementedError

    @abstractmethod
    def test_MessyCode(self):
        raise NotImplementedError

    @abstractmethod
    def test_LatestDate(self):
        raise NotImplementedError

    @abstractmethod
    def test_DateTimeSpanTest(self):
        """主要是验收时候用，看项目上线时的历史数据量是否充足"""
        raise NotImplementedError

    @abstractmethod
    def test_DateConsecutiveness(self):
        """数据的产生日期是否连续,是否有漏数据"""
        raise NotImplementedError

    def ifmessycode(self, string):
        try:
            string.encode("gb2312")
        except UnicodeEncodeError:
            return True
        return False

    def findmessycode(self, df):
        rs = []
        for i in range(0, df.shape[0]):
            for j in range(0, df.shape[1]):
                if self.ifmessycode(str(df.iloc[i, j])):
                    rs.append("第{0}行第{1}列发现乱码：{2}".format(i, j, str(df.iloc[i, j])))
        return rs

    def raiseError(self, msg):
        raise Exception(u"测试对象：%s <br />测试警告：%s" % (self.tbName, msg))

    def raiseFailure(self, msg):
        self.fail(u"测试对象：%s <br />测试失败： %s" % (self.tbName, msg))

    @staticmethod
    def ConsecutivenessNumTest(dl=[]):
        if len(dl) == 0:
            raise Exception("date list is empty!")
        else:
            ndl = [int(a) for a in dl]
            ndl.sort()
            ci = None
            for i in ndl:
                if not ci:
                    ci = i
                    continue
                else:
                    if i == ci or i == ci + 1:
                        ci = i
                        continue
                    else:
                        return [ci, i]
        return []

    @staticmethod
    def ConsecutivenessDateTest(dl=[], fmt="%Y%m%d"):
        import datetime
        if len(dl) == 0:
            raise Exception("date list is empty!")
        else:
            ndl = [datetime.datetime.strptime(str(a), fmt) for a in dl]
            ndl.sort()
            ci = None
            for i in ndl:
                if not ci:
                    ci = i
                    continue
                else:
                    if i == ci or i == ci + datetime.timedelta(days=1):
                        ci = i
                        continue
                    else:
                        return [ci, i]
        return []

    @staticmethod
    def GetLastYearSameDay(strdt, fmt="%Y-%m-%d"):
        import datetime
        dt = datetime.datetime.strptime(strdt, fmt)
        dt_lastyear = dt + datetime.timedelta(days=-365)
        return dt_lastyear.strftime(fmt)

    def run(self, result=None):
        orig_result = result
        if result is None:
            result = self.defaultTestResult()
            startTestRun = getattr(result, 'startTestRun', None)
            if startTestRun is not None:
                startTestRun()

        self._resultForDoCleanups = result
        result.startTest(self)

        testMethod = getattr(self, self._testMethodName)
        if (getattr(self.__class__, "__unittest_skip__", False) or
            getattr(testMethod, "__unittest_skip__", False)):
            # If the class or method was skipped.
            try:
                skip_why = (getattr(self.__class__, '__unittest_skip_why__', '')
                            or getattr(testMethod, '__unittest_skip_why__', ''))
                self._addSkip(result, skip_why)
            finally:
                result.stopTest(self)
            return
        try:
            success = False
            try:
                self.setUp()
            except SkipTest as e:
                self._addSkip(result, str(e))
            except KeyboardInterrupt:
                raise
            except:
                result.addError(self, sys.exc_info())
            else:
                try:
                    testMethod()
                except KeyboardInterrupt:
                    raise
                except self.failureException:
                    result.addFailure(self, sys.exc_info())
                except _ExpectedFailure as e:
                    addExpectedFailure = getattr(result, 'addExpectedFailure', None)
                    if addExpectedFailure is not None:
                        addExpectedFailure(self, e.exc_info)
                    else:
                        warnings.warn("TestResult has no addExpectedFailure method, reporting as passes",
                                      RuntimeWarning)
                        result.addSuccess(self)
                except _UnexpectedSuccess:
                    addUnexpectedSuccess = getattr(result, 'addUnexpectedSuccess', None)
                    if addUnexpectedSuccess is not None:
                        addUnexpectedSuccess(self)
                    else:
                        warnings.warn("TestResult has no addUnexpectedSuccess method, reporting as failures",
                                      RuntimeWarning)
                        result.addFailure(self, sys.exc_info())
                except SkipTest as e:
                    self._addSkip(result, str(e))
                except:
                    if u"测试警告：" in str(sys.exc_info()[1]):
                        result.addError(self, sys.exc_info())
                    else:
                        if u"测试失败：" in str(sys.exc_info()[1]):
                            result.addFailure(self, sys.exc_info())
                        else:
                            # result.addFailure(self, (Exception, "Unknown Failure!", None))
                            result.addError(self, sys.exc_info())
                else:
                    success = True

                try:
                    self.tearDown()
                except KeyboardInterrupt:
                    raise
                except:
                    result.addError(self, sys.exc_info())
                    success = False

            cleanUpSuccess = self.doCleanups()
            success = success and cleanUpSuccess
            if success:
                result.addSuccess(self)
        finally:
            result.stopTest(self)
            if orig_result is None:
                stopTestRun = getattr(result, 'stopTestRun', None)
                if stopTestRun is not None:
                    stopTestRun()
