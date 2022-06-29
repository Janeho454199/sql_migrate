#!/usr/bin/env python
"""
-------------------------------------------------
   开发人员：
   开发日期：2022-06-16
   开发工具：PyCharm
   功能描述：执行器
-------------------------------------------------
    Change Activity:

-------------------------------------------------
"""
import traceback
import datetime
import click
from migrate_main.tth_migrate import TTHConvert
from migrate_core.log import logger


CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])


@click.group(context_settings=CONTEXT_SETTINGS)
def cli():
    """ sql convertor """


@cli.command(help='运行转换，参数：-f -t')
@click.option('-f', type=str, help='input file path')
@click.option('-t', type=str, help='convert type(DDL, DML)')
def start(f, t):
    """ 启动转换程序 """
    start_time = datetime.datetime.now()
    convert = TTHConvert(convert_type=t)
    try:
        with open('.' + f, mode='r', encoding='utf-8') as file:
            sql = file.read()
            convert.start(sql, f)
            end_time = datetime.datetime.now()
            logger.info(type='Convert finished!', message='Time usage:{} seconds'.format(
                (end_time - start_time).seconds + (end_time - start_time).microseconds / 100000))
    except Exception as e:
        logger.exception(type='Convert fail!', message=traceback.format_exc())


if __name__ == '__main__':
    cli()


