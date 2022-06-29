#!/usr/bin/env python
"""
-------------------------------------------------
   开发人员：
   开发日期：2022-06-16
   开发工具：PyCharm
   功能描述：日志组件
-------------------------------------------------
    Change Activity:

-------------------------------------------------
"""
import logging


class Logger:
    """
    Token from https://github.com/gaojiuli/toapi/blob/master/toapi/log.py
    """

    def __init__(self, name, level=logging.DEBUG):
        logging.basicConfig(format='%(asctime)s %(message)-10s ',
                            datefmt='%Y/%m/%d %H:%M:%S')

        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)

    def info(self, type, message):
        self.logger.info('[%-16s] %-2s %s' % (type, 'OK', message))

    def error(self, type, message):
        self.logger.error('[%-16s] %-4s %s' % (type, 'FAIL', message))

    def exception(self, type, message):
        self.logger.error('[%-16s] %-5s %s' % (type, 'ERROR', message))


logger = Logger('spdier')
