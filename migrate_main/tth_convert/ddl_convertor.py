"""
-------------------------------------------------
   开发人员：
   开发日期：2022-06-16
   开发工具：PyCharm
   功能描述：TD转HIVE语法转换类
-------------------------------------------------
    Change Activity:

-------------------------------------------------
"""
import re
from migrate_core import DDLConvertor


class TdToHiveDDLConvertor(DDLConvertor):
    """
    DDL Convertor class
    """
    @staticmethod
    def create_convert(sql):
        """
        转换MULTISET/SET关键字
        :param sql: 待转换的sql
        :return convert_sql: 转换后的sql
        """
        rp = re.compile(r'\s(MULTISET|SET)\s', flags=re.I)
        convert_sql = rp.sub(' ', sql)
        return convert_sql

    @staticmethod
    def temporary_convert(sql):
        """
        转换VOLATILE关键字
        :param sql: 待转换的sql
        :return convert_sql: 转换后的sql
        """
        rp = re.compile(r'\s(VOLATILE)\s', flags=re.I)
        convert_sql = rp.sub(' ', sql)
        return convert_sql

    @staticmethod
    def no_log_convert(sql):
        """
        转换 NO LOG 关键字
        :param sql: 待转换的sql
        :return convert_sql: 转换后的sql
        """
        rp = re.compile(r',\s*(NO)?\s*LOG\s+[^\(]*', flags=re.I | re.S | re.M)
        convert_sql = rp.sub('', sql)
        return convert_sql

    @staticmethod
    def create_detail_convert(sql):
        """
        转换 FALLBACK , JOURNAL,  CHECKSUM, MERGEBLOCKRATIO 等定义关键字
        :param sql: 待转换的sql
        :return convert_sql: 转换后的sql
        """
        rp = re.compile(r',\s*(NO)?\s*FALLBACK\s+[^\(]+', flags=re.I | re.S | re.M)
        convert_sql = rp.sub('', sql)
        return convert_sql

    @staticmethod
    def character_set_convert(sql):
        """
        转换 CHARACTER SET 定义关键字
        :param sql: 待转换的sql
        :return convert_sql: 转换后的sql
        """
        rp = re.compile(r'CHARACTER\s+SET\s+(LATIN|UNICODE){1}(\s+)', flags=re.I)
        convert_sql = rp.sub('', sql)
        return convert_sql

    @staticmethod
    def casespecific_convert(sql):
        """
        转换 CASESPECIFIC 定义关键字
        :param sql: 待转换的sql
        :return convert_sql: 转换后的sql
        """
        rp = re.compile(r'(NOT\s)?CASESPECIFIC(\s)?', flags=re.I | re.S | re.M)
        convert_sql = rp.sub('', sql)
        return convert_sql

    @staticmethod
    def title_convert(sql):
        """
        转换 TITLE 定义关键字
        :param sql: 待转换的sql
        :return convert_sql: 转换后的sql
        """
        rp = re.compile(r"TITLE\s+\'([^']+)\'", flags=re.I)
        convert_sql = rp.sub(r"COMMENT '\g<1>'", sql)
        return convert_sql

    @staticmethod
    def date_format_convert(sql):
        """
        转换 DATE(FORMAT) 定义关键字
        :param sql: 待转换的sql
        :return convert_sql: 转换后的sql
        """
        rp = re.compile(r"DATE\s+FORMAT\s+\'([^']+)\'", flags=re.I)
        convert_sql = rp.sub('DATE', sql)
        return convert_sql

    @staticmethod
    def byteint_convert(sql):
        """
        转换 BYTEINT 定义关键字
        :param sql: 待转换的sql
        :return convert_sql: 转换后的sql
        """
        rp = re.compile(r"(^\s+[^\s]+\s+)(BYTEINT)\s", flags=re.I)
        convert_sql = rp.sub(r'\g<1>TINYINT ', sql)
        return convert_sql

    @staticmethod
    def timestamp_convert(sql):
        """
        转换 TIMESTAMP 定义关键字
        :param sql: 待转换的sql
        :return convert_sql: 转换后的sql
        """
        rp = re.compile(r"^(\s+[^\s]+)\s+TIMESTAMP(\([\d]+\))\s*(FORMAT\s+'[^']+')?\s?", flags=re.I)
        convert_sql = rp.sub(r'\g<1> TIMESTAMP ', sql)
        return convert_sql

    @staticmethod
    def primary_key_convert(sql):
        """
        转换 PRIMARY KEY 定义关键字
        :param sql: 待转换的sql
        :return convert_sql: 转换后的sql
        """
        # 去掉NO PRIMARY INDEX
        rp_1 = re.compile(r"NO\s+PRIMARY\s+INDEX", flags=re.I | re.S)
        rp_2 = re.compile(r"PRIMARY\s+INDEX\s*\(([^\)]+)\)", flags=re.I | re.S)
        convert_sql = rp_1.sub(' ', sql)
        convert_sql = rp_2.sub(' ', convert_sql)
        return convert_sql

    @staticmethod
    def compress_convert(sql):
        """
        转换 COMPRESS 定义关键字
        :param sql: 待转换的sql
        :return convert_sql: 转换后的sql
        """
        # 处理compress值
        rp_1 = re.compile(r"\s*COMPRESS\s+\(.*?\)\s*([\)\,]\s*?[\r\n][ ]{6})", flags=re.I | re.S)
        # 处理无括号类型
        rp_2 = re.compile(r"\s*COMPRESS\s+.*([\)\,])$", flags=re.I | re.M)
        # 处理压缩值换行
        rp_3 = re.compile(r"\s*COMPRESS(\s)?\(?(.*?)\)?", flags=re.I | re.S)
        convert_sql = rp_1.sub(r' \g<1>', sql)
        convert_sql = rp_2.sub(r' \g<1>', convert_sql)
        convert_sql = rp_3.sub(r' \g<1>', convert_sql)
        return convert_sql

    @staticmethod
    def partition_convert(sql):
        """
        转换 PARTITION 定义关键字
        :param sql: 待转换的sql
        :return convert_sql: 转换后的sql
        """
        rp = re.compile(r"PARTITION\s+BY\s+RANGE_N\((.*?)\)", flags=re.I | re.S)
        # 拿到分区字段，查找字段类型，以及中文注释
        pc_column = re.findall(r'PARTITION\s+BY\s+RANGE_N\(([^\s]+)', sql)[0]
        pc_type = re.findall(r'{}\s+([^\s]+)'.format(pc_column), sql)[0]
        convert_sql = rp.sub(r"PARTITIONED BY ({} {})".format(pc_column, pc_type, ), sql)
        # 将分区字段在原表中去除
        # 如果该字段在中间位置
        convert_sql = re.sub(r'.*{}.*,\n'.format(pc_column), '', convert_sql, flags=re.I)
        # 如果该字段是最后一个字段
        convert_sql = re.sub(r'.*{}.*\)(?!;)'.format(pc_column).format(pc_column), ')', convert_sql, flags=re.I)
        return convert_sql

    @staticmethod
    def commit_preserve_convert(sql):
        """
        转换 ON COMMIT PRESERVE ROWS 定义关键字
        :param sql: 待转换的sql
        :return convert_sql: 转换后的sql
        """
        rp = re.compile(r'ON COMMIT PRESERVE ROWS', flags=re.I)
        convert_sql = rp.sub('', sql)
        return convert_sql

    @staticmethod
    def table_comment_convert(sql):
        """
        转换 COMMENT ON TABLE 定义关键字
        :param sql: 待转换的sql
        :return convert_sql: 转换后的sql
        """
        rp = re.compile(r"COMMENT\s+ON\s+TABLE\s+([^\s]+)\s+IS\s+\'([^']*)\'", flags=re.I)
        convert_sql = rp.sub(r"ALTER TABLE \g<1> SET TBLPROPERTIES('comment' = '\g<2>')", sql)
        return convert_sql

    @staticmethod
    def if_errorcode_convert(sql):
        """
        转换 .IF ERRORCODE <> 0 THEN
        :param sql: 待转换的sql
        :return convert_sql: 转换后的sql
        """
        rp_1 = re.compile(r"\.IF\s+ERRORCODE(.+)\s+THEN", flags=re.I | re.M)
        rp_2 = re.compile(r"^\s*\.LABEL", flags=re.I | re.M)
        rp_3 = re.compile(r"\.QUIT\s+12\s*(\;)*\n", flags=re.I | re.S)
        rp_4 = re.compile(r"\.QUIT\s*?\n", flags=re.I | re.S)
        rp_5 = re.compile(r"\.GOTO\s+(\S+)\s*(\;)*\n", flags=re.I | re.S)
        rp_6 = re.compile(r"\.IF\s+ACTIVITYCOUNT(.+?)\s+THEN\s+([^\n]+)\n", flags=re.I | re.S)
        rp_7 = re.compile(r"\.LOGOFF.*$", flags=re.I | re.S)
        convert_sql = rp_1.sub(r"if ${} {} then".format('var:_WARNING_CODE', rp_1.findall(sql)[0][0]), sql)
        convert_sql = rp_2.sub(r"LABEL", convert_sql)
        convert_sql = rp_3.sub(r"quit_with_error\;\n", convert_sql)
        convert_sql = rp_4.sub(r"quit\;\n", convert_sql)
        convert_sql = rp_5.sub(r"goto \g<1>\;\n", convert_sql)
        convert_sql = rp_6.sub(r"if \${var:_FETCHED_ROWS} \g<1> then g<2> \;\n", convert_sql)
        convert_sql = rp_7.sub(r"quit\;\n", convert_sql)

        return convert_sql

    @staticmethod
    def char_convert(sql):
        """
        转换 CHAR/VARCHAR 类型
        :param sql: 待转换的sql
        :return: 转换后的sql
        """
        rp = re.compile(r"(CHAR|VARCHAR)\((\d*)\)", flags=re.I)
        column_list = [column[0] for column in re.findall(r'((CHAR|VARCHAR)\(\d*\))', sql, flags=re.I)]
        for column in column_list:
            # temp数组各下标说明 0: 字段类型, 1: 长度
            temp = rp.findall(column)[0]
            # char类型处理
            if int(temp[1]) != 0 and temp[0].upper() == 'CHAR' and int(temp[1]) > 255:
                temp[0] = 'STRING'
                sql = sql.replace(column, '{}({})'.format(temp[0], temp[1]))
            # varchar类型处理
            elif int(temp[1]) != 0 and temp[0].upper() == 'VARCHAR' and int(temp[1]) > 64000:
                temp[0] = 'STRING'
                sql = sql.replace(column, '{}({})'.format(temp[0], temp[1]))

        return sql

    @staticmethod
    def default_value_convert(sql):
        """
        剔除 DEFAULT 值
        :param sql: 待转换的sql
        :return: 转换后的sql
        """
        rp = re.compile(r"\s*DEFAULT\s+.*([\)\,])$", flags=re.I | re.M)
        convert_sql = rp.sub(r' \g<1>', sql)
        return convert_sql
