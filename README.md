# sql_migrate

## 环境要求

>- python==3.7.11
>- click

## 目录结构

>│├─ exec_sql（存放测试用脚本）
>│├─ ExecuteConvert.py（转换入口）
>│├─ migrate_core（转换核心）
>│├─ migrate_main（转换主类）
>│   └─ init.py
>│   └─ tth_convert（转换的处理逻辑）
>│        └── ddl_check.py（检查器）
>│        └── ddl_convertor.py（转换器）
>│        └── dml_check.py（检查器）
>│        └── dml_convertor.py（转换器）
>│        └── init.py
>│   └─ tth_migrate.py（转换的程序逻辑）
>│├─ utils（通用工具类）
>│    └─ convert_tool.py
>│   └─ init.py

## 运行

>参数：--f --t
>
>实例：python ExecuteConvert.py start -f 要转换的脚本文件 -t DDL
>
>f：脚本相对路径
>
>t：转换类型（DDL/DML）