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
import datetime
from migrate_core import DMLConvertor
from migrate_main.tth_convert.ddl_convertor import TdToHiveDDLConvertor


class TdToHiveDMLConvertor(DMLConvertor):
    """
    DML Convertor class
    """

    @staticmethod
    def cast_convert(sql):
        """
        转换 CAST 函数
        :param sql: 待转换的sql
        :return convert_sql: 转换后的sql
        """
        # DATE类型转换
        rp = re.compile(r"CAST\(.*\sAS\s(DATE).*\)", flags=re.I | re.M)
        if rp.search(sql) is not None:
            convert_sql = TdToHiveDMLConvertor.cast_date(sql)
            return convert_sql

    @staticmethod
    def cast_date(sql):
        """
        DATE 类型 CAST 函数
        :param sql: 待转换的sql
        :return convert_sql: 转换后的sql
        """
        # 判断是否含有数值

        if re.search(r'CAST\(\'(\d+)\'\sAS\sDATE.*\)', sql, flags=re.I | re.M) is not None:
            date_num = re.findall(r'CAST\(\'(\d+)\'\sAS\sDATE.*\)', sql, flags=re.I | re.M)[0]
            date = datetime.datetime.strptime(date_num, '%Y%m%d').date()
            sql = re.sub(r'(CAST\(\'(\d+)\'\sAS\sDATE.*\))', 'CAST (\'{}\' AS DATE)'.format(str(date)),
                         sql, flags=re.I | re.M)

        # 剔除FORMAT
        convert_sql = TdToHiveDDLConvertor.date_format_convert(sql)
        return convert_sql

    @staticmethod
    def reload_convert(sql):
        """
        转换 DELETE 重跑语句
        :param sql: 待转换的sql
        :return convert_sql: 转换后的sql
        """
        rp = re.compile(r"DELETE\sFROM\s\$\{(.*?)\}.([^\s]+)\sWHERE\s(.*?)\s=.*?\{(.*?)\}.*?;",
                        flags=re.I | re.M | re.S)
        convert_sql = rp.sub(r"ALTER TABLE DROP ${\g<1>}.\g<2> IF EXISTS PARTITION(\g<3> '${\g<4>}');", sql)
        return convert_sql

    @staticmethod
    def extract_convert(sql):
        """
        转换 EXTRACT 函数
        :param sql: 待转换的sql
        :return convert_sql: 转换后的sql
        """
        rp = re.compile(r"EXTRACT\s?\((YEAR|MONTH|DAY)\sFROM\s([^\s]+?\.?[^\s]+)(\s(\+|\-)\s(\d+))?\)",
                        flags=re.I | re.M | re.S)
        # 获取EXTRACT所在SQL
        extract_list = [line[0] for line in re.findall(
            r'(EXTRACT\s?\((YEAR|MONTH|DAY)\sFROM\s([^\s]+?\.?[^\s]+)(\s(\+|\-)\s\d)?\))', sql)]
        for extract in extract_list:
            # temp数组各下标说明 0: YEAR|MONTH|DAY, 1: TABLE.COLUMN, 2: ADD/SUB CONTENT, 3:ADD/SUB, 4:ADD/SUB VALUE
            temp = rp.findall(extract)[0]
            # 判断条件为当有日期的加减且加减符号存在的时候，将原语句替换为HIVE的格式
            if temp[3] and temp[4]:
                sql = sql.replace(extract, '{}(DATE_ADD({}, {}{}))'.format(temp[0], temp[1], temp[3], temp[4]))
            else:
                sql = sql.replace(extract, '{}({})'.format(temp[0], temp[1]))
        return sql

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
        rp_7 = re.compile(r"\.LOGOFF[^\n]*?\n", flags=re.I | re.S)
        if rp_1.findall(sql):
            convert_sql = rp_1.sub(r'if ${var:_WARNING_CODE}' + '{}'.format(rp_1.findall(sql)[0][0]) + 'then', sql)
        else:
            convert_sql = sql
        convert_sql = rp_2.sub(r"LABEL", convert_sql)
        convert_sql = rp_3.sub(r"quit_with_error\;\n", convert_sql)
        convert_sql = rp_4.sub(r"quit;\n", convert_sql)
        convert_sql = rp_5.sub(r"goto \g<1>\;\n", convert_sql)
        convert_sql = rp_6.sub(r"if \${var:_FETCHED_ROWS} \g<1> then g<2> \;\n", convert_sql)
        convert_sql = rp_7.sub(r"quit;\n", convert_sql)

        return convert_sql

    @staticmethod
    def qualify_convert(sql):
        """
        转换 QUALIFY 用法
        :param sql: 待转换的sql
        :return convert_sql: 转换后的sql
        """
        qualify_no = 0
        rp = re.compile(r"(.*?)(\s*QUALIFY)(\s.*)", flags=re.I | re.M | re.S)
        search_sql = rp.findall(sql)[0]
        while re.search(r'\s*QUALIFY\s+', sql, flags=re.I | re.S):
            search_sql = rp.findall(sql)[0]
            old_sql = sql
            before_q = search_sql[0]
            match_q = search_sql[1]
            after_q = search_sql[2]
            # 从qualify向前读到最近的SELECT
            select_part = TdToHiveDMLConvertor.read_till_token_rev(before_q, 'SELECT')
            # 从qualify向前读到最近的INSERT,当查询中未指定字段名时需要从INSERT列表得到字段名
            insert_part = TdToHiveDMLConvertor.read_till_token_rev(before_q, 'INSERT')
            # 从qualify向后读到本层子查询结束的地方
            remain_part = TdToHiveDMLConvertor.read_till_token(after_q, '[\;]')
            # 去掉最后的分号
            remain_part = re.sub(r'\;$', '', remain_part, flags=re.I | re.S)

            distinct_str = ""
            if re.search(r'^\s*SELECT\s+DISTINCT', select_part, flags=re.I):
                distinct_str = " DISTINCT "

            ins_stmt = ""
            if insert_part:
                str_to_process = "{}{}{}".format(insert_part, match_q, remain_part)
                cols_def, cols_name = TdToHiveDMLConvertor.get_qualify_select_col(insert_part)
                insert_part = re.findall(r'(INSERT\s+INTO)(.+?)SELECT', insert_part, flags=re.I | re.S)[0]
                ins_stmt = "{}{}".format(insert_part[0], insert_part[1])
            else:
                str_to_process = "{}{}{}".format(select_part, match_q, remain_part)
                cols_def, cols_name = TdToHiveDMLConvertor.get_qualify_select_col(select_part)

            cols_def = re.sub(r'[\n\s]+$', '', cols_def, flags=re.I | re.S)

            m1 = match_q
            m1 = re.findall(r'(\s*)QUALIFY', m1, flags=re.I)[0]
            pre_space = m1[0]
            pre_space = re.sub(r'\n', '', pre_space, flags=re.I)

            # 取QUALIFY条件各个部分
            remain_part_temp = re.findall(r'([^<>=]+)(\s*)([<|>|=]+)(\s*)(\d+)', remain_part, flags=re.I | re.S)[0]
            cond_express_left = remain_part_temp[0]
            cond_oper = remain_part_temp[2]
            cond_express_right = remain_part_temp[4]
            qualify_cond_express = "{}{}{}{}{}".format(remain_part_temp[0], remain_part_temp[1], remain_part_temp[2],
                                                       remain_part_temp[3], remain_part_temp[4])
            # 有可能group by 写在前面
            # 暂时按简单的方式进行，先不考虑group by 后复杂表达式的情况
            group_clause = ''
            if re.search(r'GROUP\s+BY\s+(.+)', select_part, flags=re.I | re.S):
                group_clause = re.findall(r'group\s+by\s+(.+)', select_part, flags=re.I | re.S)[0][0]
            if group_clause == '':
                if re.search(r'GROUP\s+BY\s+(.+)([\n\;\)])', remain_part, flags=re.I | re.S):
                    group_clause = re.findall(r'GROUP\s+BY\s+(.+)([\n\;\)])', remain_part, flags=re.I | re.S)[0][0]

            qlty_func = TdToHiveDMLConvertor.rewrite_olap_win_func(cond_express_left, group_clause)

            # 如果 QUALIFY后的条件没有包含函数，则直接转为WHERE条件
            if re.search(r'\(.*?\)', cond_express_left, flags=re.I):
                pass
            else:
                has_where = TdToHiveDMLConvertor.read_till_token_rev(select_part, 'where')
                if has_where:
                    sql = re.sub(r'(\s*)QUALIFY(\s+)([^\n]+)', r'\g<1>\nAND\g<2>\g<3>', sql, flags=re.I | re.M)
                else:
                    sql = re.sub(r'(\s*)QUALIFY(\s+)([^\n]+)', r'\g<1>\nWHERE\g<2>\g<3>', sql, flags=re.I | re.M)

            # 取SELECT FROM后的表列表
            # my $select_col;
            select_part = re.findall(r'SELECT(.+?)FROM(.+)$', select_part, flags=re.I | re.S)[0]
            if select_part[1]:
                qry_from = select_part[1]
            else:
                return search_sql
            # 从qry_from后将group by去掉
            # qualify中的group 都是用于分组和排序，不实际汇总,需移到OLAP函数中，并从后续语句去掉
            if group_clause != '':
                qry_from = re.sub(r'GROUP\s+BY\s+{}'.format(group_clause), '', qry_from, flags=re.I | re.S)

            qry_from = re.sub(r'\n([\s]+)$', '/g<1>', qry_from, flags=re.I | re.S)

            replace_str = "{ins_stmt} \
                SELECT {distinct_str} {cols_name}  \
                {pre_space}FROM ( \
                {pre_space}    SELECT {distinct_str} {cols_def} \
                {pre_space}    ,{qlty_func} as _rnk  \
                {pre_space}    FROM {qry_from} \
                {pre_space}) _sub_qry_{qualify_no} \
                {pre_space}WHERE _rnk {cond_oper} {cond_express_right}".format(ins_stmt=ins_stmt, distinct_str=distinct_str,
                                                                               cols_name=cols_name, pre_space=pre_space,
                                                                               cols_def=cols_def, qlty_func=qlty_func,
                                                                               qry_from=qry_from, qualify_no=1,
                                                                               cond_oper=cond_oper, cond_express_right=cond_express_right)

            q_select = re.escape(str_to_process)
            sql = re.sub(q_select, replace_str, sql, flags=re.I | re.S)

            if old_sql == sql:
                return sql

        return sql

    @staticmethod
    def read_till_token_rev(sql, token):
        """
        匹配至参数之前
        :param sql: 待转换的sql
        :param token: 截至标志
        """
        sql_len = len(sql)
        in_brackets = 0
        in_single_qw = 0
        seg = ""

        for i in range(sql_len - 1, -1, -1):
            c = sql[i]
            if seg == "" and c == " ":
                seg = c + seg
                continue
            if in_single_qw == 0 and c != "'":
                if c == ")":
                    in_brackets += 1
                elif c == "(":
                    if in_brackets == 0:
                        return
                    in_brackets -= 1
            elif c == "'":
                if in_single_qw > 0:
                    in_single_qw = 0
                else:
                    in_single_qw = 1

            seg = c + seg

            if re.search(r'{}'.format(token), seg, flags=re.S | re.I) and in_brackets == 0 and in_single_qw == 0:
                return seg

        return None

    @staticmethod
    def read_till_token(sql, token):
        """
        往后读取一个直到token或者)或者结束，只在本层查询读
        :param sql: 待转换的sql
        :param token: 截至标志
        """
        sql_len = len(sql)
        in_brackets = 0
        in_single_qw = 0
        seg = ""

        for i in range(sql_len):
            c = sql[i]
            if seg == "" and c == " ":
                seg = seg + c
                continue
            if in_single_qw == 0 and c != "'":
                if c == "(":
                    in_brackets += 1
                elif c == ")":
                    if in_brackets == 0:
                        return seg
                    in_brackets -= 1
            elif c == "'":
                if in_single_qw != 0:
                    in_single_qw = 0
                else:
                    in_single_qw = 1

            seg = seg + c
            if re.search(r'^{}$'.format(token), seg, flags=re.S | re.I) and in_brackets == 0 and in_single_qw == 0:
                return seg

        return seg

    @staticmethod
    def get_sel_col_def(sel_cols):
        """
        如果是最外层，则取INSERT的字段名，否则应该有字段名定义，字段名定义可能为t.col或者col
        :param sel_cols: 待转换的sql
        """
        key_word = ["NULL", "CURRENT_DATE", "CURRENT_TIME", "DATE", "CURRENT_TIMESTAMP", "END", "CURRENT_USER",
                    "YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND"]
        cols = TdToHiveDMLConvertor.separate_whole_part(sel_cols, ',')

        col_cnt = len(cols)

        col_names = {}

        for i in range(col_cnt):
            # 获取每个字段表达式，给定字段名
            # 规则，最后是一个合法标识符，不是END，且前一个单词不是运算符
            col_names[i] = ""
            col_def = cols[i]
            col_def = col_def.upper()

            parts = TdToHiveDMLConvertor.separate_whole_part(col_def, '[\\s]')
            p_cnt = len(parts)
            # 去掉最后的空格和换行
            parts[p_cnt - 1] = re.sub(r'[\n\s]+$', '', parts[p_cnt - 1])
            last_word = ''
            if re.search(r'(\w+)$', parts[p_cnt - 1]):
                last_word = re.findall(r'(\w+)$', parts[p_cnt - 1])[0]
            if last_word != '' and last_word in key_word:
                last_word = ''
            if p_cnt <= 2:
                col_names[i] = last_word
            elif parts[p_cnt-1] not in key_word:
                if parts[p_cnt - 2] == 'AS':
                    col_names[i] = last_word
                elif re.search(r'(\w+)$', parts[p_cnt - 2]):
                    col_names[i] = last_word
                elif not re.search(r'(\w+)$', parts[p_cnt - 2]):
                    col_names[i] = last_word

        return cols, col_names

    @staticmethod
    def separate_whole_part(set_str, delimit):
        str_len = len(set_str)
        in_brackets = 0
        in_single_qw = 0
        seg = ""
        seg_num = 0
        res = {}

        for i in range(str_len):
            c = set_str[i]
            if in_single_qw == 0 and c != "'":
                if c == '(':
                    in_brackets += 1
                elif c == ')':
                    in_brackets -= 1
                elif re.search(r'{}'.format(delimit), c):
                    if in_brackets == 0 and in_single_qw == 0:
                        c1 = ''
                        if i < str_len - 1:
                            c1 = set_str[i + 1]
                        # 连续分隔符当作一个
                        if re.search(r'{}'.format(delimit), c1):
                            continue
                        elif seg == "":
                            continue
                        res[seg_num] = seg
                        seg_num += 1
                        seg = ""
                        continue
            elif c == "'":
                if in_single_qw > 0:
                    in_single_qw = 0
                else:
                    in_single_qw = 1

            seg = seg + c

        if seg != "":
            res[seg_num] = seg
            seg_num += 1

        return res

    @staticmethod
    def get_qualify_select_col(sql):
        """
        取select .. from 字段定义
        :param sql: 待转换的sql
        """
        sql = re.findall(r'SELECT(.+?)FROM', sql, flags=re.I | re.S)[0]
        select_cols = sql
        select_cols = re.sub(r'^\s*DISTINCT', '', select_cols, flags=re.I)

        cols, names = TdToHiveDMLConvertor.get_sel_col_def(select_cols)

        cnt = len(names)
        need_ins_cols = 0
        no_name_list = ''
        for i in range(cnt):
            if names.get(i, 0) == 0 or names[i] == '':
                need_ins_cols += 1
                no_name_list = '({})->['.format(i) + cols[i] + ']\n'
        if need_ins_cols > 0:
            # 有个问题，这个模式匹配放在条件语句中无法返回正确值
            temp = re.findall(r'INSERT\s+INTO\s+([^\(]+?)\(([^\)]+)\)\s*SELECT(.+?)FROM', sql)[0]
            if temp[1] != '':
                ins_cols = temp[1].split(',')
                ins_col_cnt = len(ins_cols)
                for i in range(cnt):
                    if names.get(i, 0) == 0 or names[i] == '':
                        names[i] = ins_cols[i]
                        names[i] = re.sub(r'\s', '', names[i])
                        cols[i] = cols[i] + ' ' + names[i]
        sorted(cols.keys())
        sorted(names.keys())
        sel_col_str = ','.join([col for col in cols.values() if col != ''])
        sel_col_name = ','.join([name for name in names.values() if name != ''])
        # for x in cols:
        #     if cols[x] != '':
        #         sel_col_str = sel_col_str + ","
        #         sel_col_str = sel_col_str + cols[x]
        #
        # for x in names:
        #     if names[x] != '':
        #         sel_col_name = sel_col_name + ","
        #         sel_col_name = sel_col_name + names[x]
        return sel_col_str, sel_col_name

    @staticmethod
    def rewrite_olap_win_func(cond_str, group_str=''):
        """
        改写qualify中用到的函数,可能是olap也可能不是
        """
        res = ''
        func = ''
        func_exps = ''
        sum_col = ''
        order_str = ''
        # rank() over ( )
        if re.search(r'(\w+)\s*\((\w*)\)\s*over\s*\(([^\)]+)\)', cond_str, flags=re.I | re.S):
            # no need to change
            if group_str == '':
                res = cond_str
            else:
                # 如果已经是一个完整的ASNI的OLAP函数，不应该再有group by
                res = cond_str
        elif re.search(r'(\w+)\s*\(([^\)]+)\)\s*$', cond_str, flags=re.I | re.S):
            # rank() 没有OVER
            # TD rank( col) =1 ,缺省排序时DESC，与 RANK() OVER( ORDER BY COL) 缺省为ASC不同
            default_order = 'DESC'
            func = re.findall(r'(\w+)\s*\(([^\)]+)\)\s*$', cond_str, flags=re.I | re.S)[0][0]
            func_exps = re.findall(r'(\w+)\s*\(([^\)]+)\)\s*$', cond_str, flags=re.I | re.S)[0][1]
            if re.search(r'csum', func, flags=re.I):
                # CSUM(c1 , c2 desc) 改写为SUM(c1) OVER (order by c2)
                func_exps = re.findall(r'(\w+)\s*,(.+)', func_exps, flags=re.I | re.S)[0]
                sum_col = func_exps[0]
                order_str = func_exps[1]
                res = "SUM({}) OVER ( PARTITION BY {} ORDER BY {} {})".format(
                    sum_col, group_str, order_str, default_order)
            else:
                if group_str == '':
                    res = "{} () OVER ( ORDER BY {} {})".format(func, func_exps, default_order)
                else:
                    res = "{} () OVER ( PARTITION BY {} ORDER BY {} {})".format(func, group_str, func_exps, default_order)

        return res
