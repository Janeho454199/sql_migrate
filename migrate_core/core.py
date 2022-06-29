#!/usr/bin/env python
"""
-------------------------------------------------
   开发人员：
   开发日期：2022-06-16
   开发工具：PyCharm
   功能描述：转换元类
-------------------------------------------------
    Change Activity:

-------------------------------------------------
"""
import abc
from .log import logger


class ConvertCore(metaclass=abc.ABCMeta):
    """
    Transform
    """
    convert_type = None
    logger = logger

    def __init__(self):
        if getattr(self, 'convert_type', None):
            raise ValueError('Convert must have a type(DDL, DML)')
        setattr(self, 'convert_type', self.convert_type)
        setattr(self, 'logger', self.logger)

    @abc.abstractmethod
    def convert(self, **kw):
        """It is a necessary method"""
        raise NotImplementedError

    @abc.abstractmethod
    def save(self, **kw):
        """It is a necessary method"""
        raise NotImplementedError

    @abc.abstractmethod
    def start(self, **kw):
        """Start a spider"""
        raise NotImplementedError
