# -*- coding: UTF-8 -*-
import HtmlTestRunner
import unittest
import datetime
import sys
import os
import platform
reload(sys)
sys.setdefaultencoding('utf-8')

import DimTables
import FactTables

if __name__ == '__main__':
    # Dim
    from DimTables import AssortMasterTestCase
    from DimTables import CustomerTestCase
    # Fact
    from FactTables import AgWeeklySLSPlanTestCase
    from FactTables import TotalOrderTransTestCase

    suite = unittest.TestLoader().loadTestsFromTestCase(AssortMasterTestCase.AssortMasterTestCase)
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(CustomerTestCase.CustomerTestCase))

    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(AgWeeklySLSPlanTestCase.AgWeeklySLSPlanTestCase))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TotalOrderTransTestCase.TotalOrderTransTestCase))

    # suite.addTests(
    #     unittest.TestLoader().loadTestsFromTestCase(WHTestCase.WHTestCase))  # add additional test cases
    rtname = 'IT Data Quality Verification ' + datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    if platform.system() == "Linux":
        outputdir = '/home/username/dqm'
    else:
        outputdir = 'C:/Temp'
    runner = HtmlTestRunner.HTMLTestRunner(output=outputdir,
                                           combine_reports=True,
                                           report_title="数据质量监测报告",
                                           report_name=rtname,
                                           add_timestamp=False)
    runner.run(suite)

    ifChinese = True
    if ifChinese:
        print u"测试完毕, 将测试报告汉化."
        testcasemap = {
            "TableTestCase.TableTestCase": "基类表测试",
            "DimTables.CustomerTestCase.CustomerTestCase": "Customer维度表测试",

            # plan
            "FactTables.AgWeeklySLSPlanTestCase.AgWeeklySLSPlanTestCase": "门店别周别sum别销售计划表测试",
            "FactTables.TotalOrderTransTestCase.TotalOrderTransTestCase": "全渠道交易表测试"
        }

        testmethodmap = {
            "test_DateTimeSpanTest": "历史数据完整性测试",
            "test_LatestDate": "最新数据是否已导入",
            "test_MessyCode": "乱码检测",
            "test_RowCount": "数据行数测试",
            "test_DateConsecutiveness": "数据日期连续性测试",
            "test_zBizRule1": "业务规则测试1 ",
            "test_zBizRule2": "业务规则测试2 ",
            "test_zBizRule3": "业务规则测试3 ",
            "test_zBizRule4": "业务规则测试4 ",
            "test_zBizRule5": "业务规则测试5 ",
            "test_zBizRule6": "业务规则测试6 ",
            "test_zBizRule7": "业务规则测试7 ",
            "test_zBizRule8": "业务规则测试8 ",
            "test_zBizRule9": "业务规则测试9 ",
            "test_epaydataontime": "各店epay数据是否及时导入"
        }

        fp = outputdir + os.path.sep + rtname + ".html"

        with open(fp, 'r') as f:
            s = f.read()
            s = s.replace("<span class=\"label label-warning\" style=\"display:block;width:40px;\">Error</span>",
                          "<span class=\"label label-warning\" style=\"display:block;width:40px;\">Warn</span>")
            s = s.replace("Error:", "Warning:")
            s = s.replace("https://ajax.googleapis.com/ajax/libs/jquery/2.2.4/jquery.min.js",
                          "http://code.jquery.com/jquery-2.2.4.js")
            s = s.replace("AssertionWarning: ", "")
            s = s.replace("Exception: 测试警告：", "测试警告：")
            s = s.replace("Exception: 测试对象：", "测试对象：")
            for k in testcasemap.keys():
                s = s.replace("<th>" + k + "</th>", "<th>" + testcasemap[k] + "</th>")
            for k in testmethodmap.keys():
                s = s.replace("<td class=\"col-xs-10\">" + k + "</td>", "<td class=\"col-xs-10\">" + testmethodmap[k] +
                              "</td>")
            if not(len(sys.argv) > 1 and sys.argv[1] == '-v'):
                import re
                s = re.sub(r"""<p style="color:maroon;">Traceback[\s\S]*?</p>""", "", s)

        with open(fp, 'w') as f:
            f.write(s)

        print u"汉化完毕."
