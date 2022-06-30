#!/usr/bin/env python
"""
-------------------------------------------------
   开发人员：
   开发日期：2022-06-16
   开发工具：PyCharm
   功能描述：执行器
-------------------------------------------------
    Change Activity:
                    2022-06-30:添加chardet库，自动识别文件编码格式
-------------------------------------------------
"""
import traceback
import datetime
import click
import chardet
from chardet.universaldetector import UniversalDetector
from migrate_main.tth_migrate import TTHConvert
from migrate_core.log import logger


CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])


def get_encoding(file_path):
    """
    判断文件编码格式
    :param file_path: 文件相对路径
    :return : 返回文件编码
    """
    # 这个方式一下子读完，然后进行判断，但不适合大文件。
    # with open('.' + file_path, 'rb') as f:
    #     return chardet.detect(f.read())['encoding']
    # 选择对读取的数据进行分块迭代，
    # 每次迭代出的数据喂给detector，当喂给detector数据达到一定程度足以进行高准确性判断时，detector.done返回True。
    # 此时我们就可以获取该文件的编码格式。
    detector = UniversalDetector()
    with open('.' + file_path, 'rb') as f:
        for line in f.readlines():
            detector.feed(line)
            if detector.done:
                break
        detector.close()
        return detector.result['encoding']


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
    encoding = get_encoding(f)
    try:
        with open('.' + f, mode='r', encoding=encoding) as file:
            sql = file.read()
            convert.start(sql, f)
            end_time = datetime.datetime.now()
            logger.info(type='Convert finished!', message='Time usage:{} seconds'.format(
                (end_time - start_time).seconds + (end_time - start_time).microseconds / 100000))
    except Exception as e:
        logger.exception(type='Convert fail!', message=traceback.format_exc())


if __name__ == '__main__':
    cli()


