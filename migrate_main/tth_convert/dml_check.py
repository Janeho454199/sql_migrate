"""
-------------------------------------------------
   开发人员：
   开发日期：2022-06-16
   开发工具：PyCharm
   功能描述：TD转HIVE语法检查类
-------------------------------------------------
    Change Activity:

-------------------------------------------------
"""
import re
from migrate_core import DMLCheck


class TdToHiveDMLCheck(DMLCheck):
    """
    DML Check class
    """
    @staticmethod
    def cast_check(sql):
        """
        判断是否含有 CAST 函数
        :param sql: 待检查的sql
        :return: 检查对应转换的方法名, 未匹配上则返回None
        """
        if re.search(r'CAST\(.*?\)', sql, flags=re.I | re.S | re.M) is not None:
            return 'cast_convert'

    @staticmethod
    def reload_check(sql):
        """
        判断是否含有 DELETE 重跑
        :param sql: 待检查的sql
        :return: 检查对应转换的方法名, 未匹配上则返回None
        """
        if re.search(r'DELETE\sFROM\s\$\{.*?\}\.?([^\s]+)', sql, flags=re.I) is not None:
            return 'reload_convert'

    @staticmethod
    def extract_check(sql):
        """
        判断是否含有 EXTRACT 函数
        :param sql: 待检查的sql
        :return: 检查对应转换的方法名, 未匹配上则返回None
        """
        if re.search(r'EXTRACT\s?\(.*?\)', sql, flags=re.I) is not None:
            return 'extract_convert'

    @staticmethod
    def qualify_check(sql):
        """
        判断是否含有 QUALIFY用法 函数
        :param sql: 待检查的sql
        :return: 检查对应转换的方法名, 未匹配上则返回None
        """
        if re.search(r'\s*QUALIFY\s+', sql, flags=re.I | re.S) is not None:
            return 'qualify_convert'

    @staticmethod
    def if_errorcode_check(sql):
        """
        判断是否含有 .IF ERRORCODE <> 0 THEN 等定义
        :param sql: 待检查的sql
        :return: 检查对应转换的方法名, 未匹配上则返回None
        """
        rp_1 = re.compile(r"\.IF\s+ERRORCODE(.+)\s+THEN", flags=re.I | re.M)
        rp_2 = re.compile(r"^\s*\.LABEL", flags=re.I | re.M)
        rp_3 = re.compile(r"\.QUIT\s+12\s*(\;)*\n", flags=re.I | re.S)
        rp_4 = re.compile(r"\.QUIT\s*?\n", flags=re.I | re.S)
        rp_5 = re.compile(r"\.GOTO\s+(\S+)\s*(\;)*\n", flags=re.I | re.S)
        rp_6 = re.compile(r"\.IF\s+ACTIVITYCOUNT(.+?)\s+THEN\s+([^\n]+)\n", flags=re.I | re.S)
        rp_7 = re.compile(r"\.LOGOFF\s*?\n", flags=re.I | re.S)

        if rp_1.search(sql) is not None:
            return 'if_errorcode_convert'
        elif rp_2.search(sql) is not None:
            return 'if_errorcode_convert'
        elif rp_3.search(sql) is not None:
            return 'if_errorcode_convert'
        elif rp_4.search(sql) is not None:
            return 'if_errorcode_convert'
        elif rp_5.search(sql) is not None:
            return 'if_errorcode_convert'
        elif rp_6.search(sql) is not None:
            return 'if_errorcode_convert'
        elif rp_7.search(sql) is not None:
            return 'if_errorcode_convert'
