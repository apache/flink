---
title: "Table API 教程"
weight: 21
type: docs
aliases:
  - /zh/dev/python/table_api_tutorial.html
  - /zh/tutorials/python_table_api.html
  - /zh/getting-started/walkthroughs/python_table_api.html
  - /zh/try-flink/python_table_api.html
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Table API 教程

Apache Flink 提供 Table API 关系型 API 来统一处理流和批，即查询在无边界的实时流或有边界的批处理数据集上以相同的语义执行，并产生相同的结果。 Flink 的 Table API 易于编写，通常能简化数据分析，数据管道和ETL应用的编码。

## 概要

在该教程中，我们会从零开始，介绍如何创建一个 Flink Python 项目及运行 Python Table API 作业。该作业读取一个 csv 文件，计算词频，并将结果写到一个结果文件中。

## 先决条件

本练习假定你对 Python 有一定的了解，但是即使你来自其他编程语言，也应该能够继续学习。
它还假定你熟悉基本的关系操作，例如 `SELECT` 和 `GROUP BY` 子句。

## 如何寻求帮助

如果你遇到问题，可以访问 [社区信息页面](https://flink.apache.org/zh/community.html)。
与此同时，Apache Flink 的[用户邮件列表](https://flink.apache.org/zh/community.html#mailing-lists) 一直被列为 Apache 项目中最活跃的项目邮件列表之一，也是快速获得帮助的好方法。

## 继续我们的旅程

如果要继续我们的旅程，你需要一台具有以下功能的计算机：

* Java 11
* Python 3.8, 3.9 or 3.10

使用 Python Table API 需要安装 PyFlink，它已经被发布到 [PyPi](https://pypi.org/project/apache-flink/)，你可以通过如下方式安装 PyFlink：

```bash
$ python -m pip install apache-flink
```

安装 PyFlink 后，你便可以编写 Python Table API 作业了。

## 编写一个 Flink Python Table API 程序

编写 Flink Python Table API 程序的第一步是创建 `TableEnvironment`。这是 Python Table API 作业的入口类。

```python
t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
t_env.get_config().set("parallelism.default", "1")
```

接下来，我们将介绍如何创建源表和结果表。

```python
t_env.create_temporary_table(
    'source',
    TableDescriptor.for_connector('filesystem')
        .schema(Schema.new_builder()
                .column('word', DataTypes.STRING())
                .build())
        .option('path', input_path)
        .format('csv')
        .build())
tab = t_env.from_path('source')

t_env.create_temporary_table(
    'sink',
    TableDescriptor.for_connector('filesystem')
        .schema(Schema.new_builder()
                .column('word', DataTypes.STRING())
                .column('count', DataTypes.BIGINT())
                .build())
        .option('path', output_path)
        .format(FormatDescriptor.for_format('canal-json')
                .build())
        .build())
```

你也可以使用 `TableEnvironment.execute_sql()` 方法，通过 DDL 语句来注册源表和结果表:
```python
my_source_ddl = """
    create table source (
        word STRING
    ) with (
        'connector' = 'filesystem',
        'format' = 'csv',
        'path' = '{}'
    )
""".format(input_path)

my_sink_ddl = """
    create table sink (
        word STRING,
        `count` BIGINT
    ) with (
        'connector' = 'filesystem',
        'format' = 'canal-json',
        'path' = '{}'
    )
""".format(output_path)

t_env.execute_sql(my_source_ddl)
t_env.execute_sql(my_sink_ddl)
```

上面的程序展示了如何创建及注册表名分别为 `source` 和 `sink` 的表。
其中，源表 `source` 有一列: word，该表代表了从 `input_path` 所指定的输入文件中读取的单词；
结果表 `sink` 有两列: word 和 count，该表的结果会输出到 `output_path` 所指定的输出文件中。

接下来，我们介绍如何创建一个作业：该作业读取表 `source` 中的数据，进行一些变换，然后将结果写入表 `sink`。

最后，需要做的就是启动 Flink Python Table API 作业。上面所有的操作，比如创建源表
进行变换以及写入结果表的操作都只是构建作业逻辑图，只有当 `execute_insert(sink_name)` 被调用的时候，
作业才会被真正提交到集群或者本地进行执行。

```python
@udtf(result_types=[DataTypes.STRING()])
def split(line: Row):
    for s in line[0].split():
        yield Row(s)

# 计算 word count
tab.flat_map(split).alias('word') \
    .group_by(col('word')) \
    .select(col('word'), lit(1).count) \
    .execute_insert('sink') \
    .wait()
```

该教程的完整代码如下:

```python
import argparse
import logging
import sys

from pyflink.common import Row
from pyflink.table import (EnvironmentSettings, TableEnvironment, TableDescriptor, Schema,
                           DataTypes, FormatDescriptor)
from pyflink.table.expressions import lit, col
from pyflink.table.udf import udtf

word_count_data = ["To be, or not to be,--that is the question:--",
                   "Whether 'tis nobler in the mind to suffer",
                   "The slings and arrows of outrageous fortune",
                   "Or to take arms against a sea of troubles,",
                   "And by opposing end them?--To die,--to sleep,--",
                   "No more; and by a sleep to say we end",
                   "The heartache, and the thousand natural shocks",
                   "That flesh is heir to,--'tis a consummation",
                   "Devoutly to be wish'd. To die,--to sleep;--",
                   "To sleep! perchance to dream:--ay, there's the rub;",
                   "For in that sleep of death what dreams may come,",
                   "When we have shuffled off this mortal coil,",
                   "Must give us pause: there's the respect",
                   "That makes calamity of so long life;",
                   "For who would bear the whips and scorns of time,",
                   "The oppressor's wrong, the proud man's contumely,",
                   "The pangs of despis'd love, the law's delay,",
                   "The insolence of office, and the spurns",
                   "That patient merit of the unworthy takes,",
                   "When he himself might his quietus make",
                   "With a bare bodkin? who would these fardels bear,",
                   "To grunt and sweat under a weary life,",
                   "But that the dread of something after death,--",
                   "The undiscover'd country, from whose bourn",
                   "No traveller returns,--puzzles the will,",
                   "And makes us rather bear those ills we have",
                   "Than fly to others that we know not of?",
                   "Thus conscience does make cowards of us all;",
                   "And thus the native hue of resolution",
                   "Is sicklied o'er with the pale cast of thought;",
                   "And enterprises of great pith and moment,",
                   "With this regard, their currents turn awry,",
                   "And lose the name of action.--Soft you now!",
                   "The fair Ophelia!--Nymph, in thy orisons",
                   "Be all my sins remember'd."]


def word_count(input_path, output_path):
    t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
    # write all the data to one file
    t_env.get_config().set("parallelism.default", "1")

    # define the source
    if input_path is not None:
        t_env.create_temporary_table(
            'source',
            TableDescriptor.for_connector('filesystem')
                .schema(Schema.new_builder()
                        .column('word', DataTypes.STRING())
                        .build())
                .option('path', input_path)
                .format('csv')
                .build())
        tab = t_env.from_path('source')
    else:
        print("Executing word_count example with default input data set.")
        print("Use --input to specify file input.")
        tab = t_env.from_elements(map(lambda i: (i,), word_count_data),
                                  DataTypes.ROW([DataTypes.FIELD('line', DataTypes.STRING())]))

    # define the sink
    if output_path is not None:
        t_env.create_temporary_table(
            'sink',
            TableDescriptor.for_connector('filesystem')
                .schema(Schema.new_builder()
                        .column('word', DataTypes.STRING())
                        .column('count', DataTypes.BIGINT())
                        .build())
                .option('path', output_path)
                .format(FormatDescriptor.for_format('canal-json')
                        .build())
                .build())
    else:
        print("Printing result to stdout. Use --output to specify output path.")
        t_env.create_temporary_table(
            'sink',
            TableDescriptor.for_connector('print')
                .schema(Schema.new_builder()
                        .column('word', DataTypes.STRING())
                        .column('count', DataTypes.BIGINT())
                        .build())
                .build())

    @udtf(result_types=[DataTypes.STRING()])
    def split(line: Row):
        for s in line[0].split():
            yield Row(s)

    # compute word count
    tab.flat_map(split).alias('word') \
        .group_by(col('word')) \
        .select(col('word'), lit(1).count) \
        .execute_insert('sink') \
        .wait()
    # remove .wait if submitting to a remote cluster, refer to
    # https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/python/faq/#wait-for-jobs-to-finish-when-executing-jobs-in-mini-cluster
    # for more details


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        required=False,
        help='Input file to process.')
    parser.add_argument(
        '--output',
        dest='output',
        required=False,
        help='Output file to write results to.')

    argv = sys.argv[1:]
    known_args, _ = parser.parse_known_args(argv)

    word_count(known_args.input, known_args.output)
```

## 执行一个 Flink Python Table API 程序

接下来，可以在命令行中运行作业（假设作业名为 word_count.py）：

```bash
$ python word_count.py
```

上述命令会构建 Python Table API 程序，并在本地 mini cluster 中运行。如果想将作业提交到远端集群执行，
可以参考[作业提交示例]({{< ref "docs/deployment/cli" >}}#submitting-pyflink-jobs)。

最后，你可以得到如下运行结果：

```bash
+I[To, 1]
+I[be,, 1]
+I[or, 1]
+I[not, 1]
...
```

上述教程介绍了如何编写并运行一个 Flink Python Table API 程序，你也可以访问 {{< gh_link file="flink-python/pyflink/examples" name="PyFlink 示例" >}}，了解更多关于 PyFlink 的示例。
如果想了解 Flink Python Table API 的更多信息，可以参考{{< pythondoc name="Flink Python API 文档">}}。
