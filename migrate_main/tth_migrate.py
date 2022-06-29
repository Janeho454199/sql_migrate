#!/usr/bin/env python
"""
-------------------------------------------------
   开发人员：
   开发日期：2022-06-16
   开发工具：PyCharm
   功能描述：TD转HIVE语法主类
-------------------------------------------------
    Change Activity:

-------------------------------------------------
"""
import re
import os
import socket
import inspect
import traceback
from abc import ABC
from operator import methodcaller
from migrate_core import ConvertCore
from .tth_convert import TdToHiveDDLCheck, TdToHiveDDLConvertor, TdToHiveDMLCheck, TdToHiveDMLConvertor
from utils import TdToHive


class TTHConvert(ConvertCore, ABC):

    def __init__(self, convert_type):
        """
        convert init
        :param convert_type: convert type
        :return:
        """
        if convert_type not in ['DDL', 'DML']:
            raise ValueError('Unsupported type')
        super().__init__()
        self.convert_type = convert_type
        self.convert_method = {
            'DDL': self.ddl_convert,
            'DML': self.dml_convert
        }
        # 日志与返回结果
        self.result_json = {
            # 机器名称 + 进程号
            'hostname': str(socket.gethostname()) + '-' + str(os.getpid()),
            'result': 1,
            'data': '',
            'error_msg': '',
        }

    @staticmethod
    def ddl_convert(ddl_sql):
        """
        ddl convert sql
        :param ddl_sql:
        :return:
        """
        # sql_list
        sql_list = []
        # 按表进行处理
        ddl_sql = re.findall(r'(.*?;)$', ddl_sql, flags=re.I | re.S | re.M)
        # 获取检查列表
        check_list = inspect.getmembers(TdToHiveDDLCheck, inspect.isfunction)
        for sql in ddl_sql:
            # 处理注释
            sql = TdToHive.replace_comment(sql)
            need_convert_list = []
            # 执行检查
            for check in check_list:
                checked = check[1](sql)
                # 添加需要进行转换的方法
                if checked:
                    need_convert_list.append(checked)

            # 执行转换
            for convert in need_convert_list:
                sql = methodcaller(convert, sql)(TdToHiveDDLConvertor)

            # NOT NULL置前
            sql = TdToHive.prev_not_null(sql)
            # 添加固定格式
            sql = TdToHive.add_row_format(',', r'\n', sql)
            sql = TdToHive.add_store('PARQUET', sql)
            # sql = TdToHive.add_location('/poc_test/dms_poc_hdfs', sql)
            sql_list.append(sql)

        return sql_list

    @staticmethod
    def dml_convert(dml_sql):
        """
        dml convert sql
        :param dml_sql:
        :return:
        """
        # 按每条语句进行处理
        sql_list = []
        dml_sql = re.findall(r'(.*?;)$', dml_sql, flags=re.I | re.S | re.M)
        # 获取检查列表
        check_list = inspect.getmembers(TdToHiveDMLCheck, inspect.isfunction)
        for sql in dml_sql:
            # 处理注释
            sql = TdToHive.replace_comment(sql)
            need_convert_list = []
            # 执行检查
            for check in check_list:
                checked = check[1](sql)
                # 添加需要进行转换的方法
                if checked:
                    need_convert_list.append(checked)

            # 执行转换
            for convert in need_convert_list:
                sql = methodcaller(convert, sql)(TdToHiveDMLConvertor)
            sql_list.append(sql)

        return sql_list

    def convert(self, sql):
        """
        convert sql
        :param sql: 待转换SQL
        :return:
        """
        convert_function = self.convert_method[self.convert_type]
        convert_list = convert_function(sql)
        convert_result = ''.join(convert_list)
        return convert_result

    def save(self, output_file, sql):
        """
        输出转换结果与日志
        :param sql: 转换完成的SQL
        :param output_file: 输出路径
        """
        with open('.' + output_file + '_after', mode="w", encoding='utf-8') as file:
            file.write(sql)

    def start(self, sql, output_file):
        """
        读取文件执行转换
        :param sql: 待转换的SQL
        :param output_file: 输出路径
        :return:
        """
        try:
            self.result_json['data'] = self.convert(sql)
            self.save(output_file, self.result_json['data'])
        except Exception as e:
            self.result_json['result'] = 0
            self.result_json['error_msg'] = traceback.format_exc()
            raise e

