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
from migrate_core import DDLCheck


class TdToHiveDDLCheck(DDLCheck):
    """
    DDL Check class
    """
    @staticmethod
    def create_check(sql):
        """
        判断是否Create开头
        :param sql: 待检查的sql
        :return: 检查对应转换的方法名, 未匹配上则返回None
        """
        if re.search(r'(Create)\s+(MULTISET|SET)?\s(VOLATILE\s+)?\s?(Table)\s+([^\s]+)\.([^\s]+)', sql,
                     flags=re.I) is not None:
            return 'create_convert'
        elif re.search(r'(Create)\s+(MULTISET|SET)?\s(VOLATILE\s+)?\s?(Table)\s+([^\s]+)', sql,
                       flags=re.I) is not None:
            return 'create_convert'

    @staticmethod
    def temporary_check(sql):
        """
        判断是否存在VOLATILE
        :param sql: 待检查的sql
        :return: 检查对应转换的方法名, 未匹配上则返回None
        """
        if re.search(r'(Create)\s+(MULTISET|SET)?\s(VOLATILE)+\s*(Table)', sql, flags=re.I) is not None:
            return 'temporary_convert'

    @staticmethod
    def no_log_check(sql):
        """
        判断是否含有 No Log
        :param sql: 待检查的sql
        :return: 检查对应转换的方法名, 未匹配上则返回None
        """
        if re.search(r',\s*(NO)?\s*LOG\s+[^\(]*\(', sql, flags=re.I | re.S | re.M) is not None:
            return 'no_log_convert'

    @staticmethod
    def create_detail_check(sql):
        """
        判断是否含有 FALLBACK , JOURNAL,  CHECKSUM, MERGEBLOCKRATIO 等定义
        :param sql: 待检查的sql
        :return: 检查对应转换的方法名, 未匹配上则返回None
        """
        if re.search(r',\s*(NO)?\s*FALLBACK\s+[^\(]+\(', sql, flags=re.I | re.S | re.M) is not None:
            return 'create_detail_convert'

    @staticmethod
    def character_set_check(sql):
        """
        判断是否含有 CHARACTER SET 定义
        :param sql: 待检查的sql
        :return: 检查对应转换的方法名, 未匹配上则返回None
        """
        if re.search(r'CHARACTER\s+SET\s+(LATIN|UNICODE){1}(\s+)', sql, flags=re.I) is not None:
            return 'character_set_convert'

    @staticmethod
    def casespecific_check(sql):
        """
        判断是否含有 CASESPECIFIC 定义
        :param sql: 待检查的sql
        :return: 检查对应转换的方法名, 未匹配上则返回None
        """
        if re.search(r'((NOT\s)?CASESPECIFIC)', sql, flags=re.I) is not None:
            return 'casespecific_convert'

    @staticmethod
    def title_check(sql):
        """
        判断是否含有 TITLE 定义
        :param sql: 待检查的sql
        :return: 检查对应转换的方法名, 未匹配上则返回None
        """
        if re.search(r"TITLE\s+\'([^']+)\'", sql, flags=re.I | re.M) is not None:
            return 'title_convert'

    @staticmethod
    def date_format_check(sql):
        """
        判断是否含有 DATE_FORMAT 定义
        :param sql: 待检查的sql
        :return: 检查对应转换的方法名, 未匹配上则返回None
        """
        if re.search(r"DATE\s+FORMAT\s+\'([^']+)\'", sql, flags=re.I) is not None:
            return 'date_format_convert'

    @staticmethod
    def byteint_check(sql):
        """
        判断是否含有 BYTEINT 定义
        :param sql: 待检查的sql
        :return: 检查对应转换的方法名, 未匹配上则返回None
        """
        if re.search(r'^(\s+[^\s]+)\s+BYTEINT\s', sql, flags=re.I) is not None:
            return 'byteint_convert'

    @staticmethod
    def timestamp_check(sql):
        """
        判断是否含有 NUMBER 定义
        :param sql: 待检查的sql
        :return: 检查对应转换的方法名, 未匹配上则返回None
        """
        if re.search(r"^(\s+[^\s]+)\s+TIMESTAMP\(([\d]+)\)\s*(FORMAT\s+'[^']+')?", sql,
                     flags=re.I) is not None:
            return 'timestamp_convert'

    @staticmethod
    def compress_check(sql):
        """
        判断是否含有 COMPRESS 定义
        :param sql: 待检查的sql
        :return: 检查对应转换的方法名, 未匹配上则返回None
        """
        if re.search(r"\s*COMPRESS(\s)?\((.*?)\)", sql, flags=re.I | re.S | re.M) is not None:
            return 'compress_convert'

    @staticmethod
    def primary_key_check(sql):
        """
        判断是否含有 PRIMARY INDEX 定义
        :param sql: 待检查的sql
        :return: 检查对应转换的方法名, 未匹配上则返回None
        """
        if re.search(r"PRIMARY\s+INDEX[^\(]+\(([^\)]+)\)", sql, flags=re.I) is not None:
            return 'primary_key_convert'

    @staticmethod
    def partition_check(sql):
        """
        判断是否含有 PARTITION 定义
        :param sql: 待检查的sql
        :return: 检查对应转换的方法名, 未匹配上则返回None
        """
        if re.search(r"\sPARTITION\s+BY\s+RANGE_N\((.*?)\)", sql, flags=re.I | re.S) is not None:
            return 'partition_convert'

    @staticmethod
    def commit_preserve_check(sql):
        """
        判断是否含有 ON COMMIT PRESERVE ROWS 定义
        :param sql: 待检查的sql
        :return: 检查对应转换的方法名, 未匹配上则返回None
        """
        if re.search(r"ON COMMIT PRESERVE ROWS", sql, flags=re.I) is not None:
            return 'commit_preserve_convert'

    @staticmethod
    def table_comment_check(sql):
        """
        判断是否含有 COMMENT ON TABLE 定义
        :param sql: 待检查的sql
        :return: 检查对应转换的方法名, 未匹配上则返回None
        """
        if re.search(r"COMMENT\s+ON\s+TABLE\s+([^\s]+)\s+IS\s+\'([^']*)\'", sql, flags=re.I) is not None:
            return 'table_comment_convert'

    @staticmethod
    def if_errorcode_check(sql):
        """
        判断是否含有 .IF ERRORCODE <> 0 THEN 定义
        :param sql: 待检查的sql
        :return: 检查对应转换的方法名, 未匹配上则返回None
        """
        if re.search(r"\.IF\s+ERRORCODE\s+([^\;]+)\;", sql, flags=re.I) is not None:
            return 'if_errorcode_convert'

    @staticmethod
    def char_check(sql):
        """
        判断是否含有 CHAR 定义
        :param sql: 待检查的sql
        :return: 检查对应转换的方法名, 未匹配上则返回None
        """
        if re.search(r"(char|varchar)\(\d*\)", sql, flags=re.I) is not None:
            return 'char_convert'

    @staticmethod
    def default_value_check(sql):
        """
        判断是否含有 DEFAULT 定义
        :param sql: 待检查的sql
        :return: 检查对应转换的方法名, 未匹配上则返回None
        """
        if re.search(r"", sql, flags=re.I) is not None:
            return 'default_value_convert'
