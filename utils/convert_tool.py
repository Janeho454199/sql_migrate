#!/usr/bin/env python
"""
-------------------------------------------------
   开发人员：
   开发日期：2022-06-20
   开发工具：PyCharm
   功能描述：公共转换方法
-------------------------------------------------
    Change Activity:
        2022-06-23: 添加注释处理
-------------------------------------------------
"""
import re


class TdToHive(object):

    @staticmethod
    def add_store(store_type, sql):
        """
        添加store as语句
        :param store_type: 存储类型
        :param sql: 待转换SQL
        :return:
        """
        rp = re.compile(r"(LINES\sTERMINATED BY\s\'.*?\')(\s)?", flags=re.I | re.S | re.M)
        convert_sql = rp.sub(r'\g<1> \nSTORED AS {} TBLPROPERTIES("parquet.compress"="snappy")'.format(store_type), sql)
        return convert_sql

    @staticmethod
    def add_row_format(field, line, sql):
        """
        添加分隔符，换行符
        :param field: 分隔符
        :param line: 换行符
        :param sql: 待转换SQL
        :return: 转换后SQL
        """
        rp = re.compile(r"(PARTITIONED\s+BY\s+\(.*?\))(\s)?", flags=re.I | re.M)
        convert_sql = rp.sub(r"\g<1> \nROW FORMAT DELIMITED FIELDS TERMINATED BY '{}' LINES TERMINATED BY '\{}'".format(
            field, line), sql)
        return convert_sql

    @staticmethod
    def add_location(hdfs_path, sql):
        """
        添加HDFS路径
        :param hdfs_path: HDFS PATH
        :param sql: 待转换SQL
        :return: 转换后SQL
        """
        rp = re.compile(r"(STORED\s+AS\s+[^;]*)", flags=re.I | re.M)
        convert_sql = rp.sub(r"\g<1> LOCATION '{}'".format(hdfs_path), sql)
        return convert_sql

    @staticmethod
    def prev_not_null(sql):
        """
        把NOT NULL置前
        :param sql: 待转换SQL
        :return: 转换后SQL
        """
        all_comment = re.compile(r"(\sCOMMENT\s\'(.*)\'(\sNOT NULL))", flags=re.I | re.M)
        rp = re.compile(r"(\sCOMMENT\s\'(.*)\'(\sNOT NULL))", flags=re.I | re.M)
        comment_list = [line[0] for line in all_comment.findall(sql)]

        for comment in comment_list:
            # temp数组各下标说明 0: 完整匹配的内容, 1: 注释，2: 是否为空
            temp = rp.findall(comment)[0]
            # 判断是否非空
            if temp[2] is not None:
                sql = sql.replace(comment, " NOT NULL COMMENT '{}'".format(temp[1]))
            else:
                sql = sql.replace(comment, " COMMENT '{}'".format(temp[1]))
        return sql

    @staticmethod
    def replace_comment(sql):
        """
        替换注释
        :param sql: 带转换SQL
        :return: 转换后SQL
        """
        rp = re.compile(r"--([^\s]+)", flags=re.I)
        convert_sql = rp.sub(r"-- \g<1>", sql)
        return convert_sql
