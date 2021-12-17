---
title: "DataStream API 教程"
weight: 22
type: docs
aliases:
  - /zh/dev/python/datastream_tutorial.html
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

# DataStream API 教程

Apache Flink 提供了 DataStream API，用于构建健壮的、有状态的流式应用程序。它提供了对状态和时间细粒度控制，从而允许实现高级事件驱动系统。
在这篇教程中，你将学习如何使用 PyFlink 和 DataStream API 构建一个简单的流式应用程序。

## 你要搭建一个什么系统

在本教程中，你将学习如何编写一个简单的 Python DataStream 作业。
该程序读取一个 csv 文件，计算词频，并将结果写到一个结果文件中。

## 准备条件

本教程假设你对 Python 有一定的了解，但是即使你使用的是其它编程语言，你也应该能够学会。

## 困难求助

如果你有疑惑，可以查阅 [社区支持资源](https://flink.apache.org/zh/community.html)。
特别是，Apache Flink [用户邮件列表](https://flink.apache.org/zh/community.html#mailing-lists) 一直被评为所有 Apache 项目中最活跃的一个，也是快速获得帮助的好方法。

## 怎样跟着教程练习

首先，你需要在你的电脑上准备以下环境：

* Java 8 or 11
* Python 3.6, 3.7 or 3.8

使用 Python DataStream API 需要安装 PyFlink，PyFlink 发布在 [PyPI](https://pypi.org/project/apache-flink/)上，可以通过 `pip` 快速安装。 

```bash
$ python -m pip install apache-flink
```

一旦 PyFlink 安装完成之后，你就可以开始编写 Python DataStream 作业了。

## 编写一个 Flink Python DataStream API 程序

DataStream API 应用程序首先需要声明一个执行环境（`StreamExecutionEnvironment`），这是流式程序执行的上下文。你将通过它来设置作业的属性（例如默认并发度、重启策略等）、创建源、并最终触发作业的执行。

```python
env = StreamExecutionEnvironment.get_execution_environment()
env.set_runtime_mode(RuntimeExecutionMode.BATCH)
env.set_parallelism(1)
```

一旦创建了 `StreamExecutionEnvironment` 之后，你可以使用它来声明数据源。数据源从外部系统（如 Apache Kafka、Rabbit MQ 或 Apache Pulsar）拉取数据到 Flink 作业里。

为了简单起见，本教程读取文件作为数据源。

```python
ds = env.from_source(
    source=FileSource.for_record_stream_format(StreamFormat.text_line_format(),
                                               input_path)
                     .process_static_file_set().build(),
    watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
    source_name="file_source"
)
```

你现在可以在这个数据流上执行转换操作，或者使用 _sink_ 将数据写入外部系统。本教程使用 `FileSink` 将结果数据写入文件中。

```python
ds.sink_to(
    sink=FileSink.for_row_format(
        base_path=output_path,
        encoder=Encoder.simple_string_encoder())
    .with_output_file_config(
        OutputFileConfig.builder()
        .with_part_prefix("prefix")
        .with_part_suffix(".ext")
        .build())
    .with_rolling_policy(RollingPolicy.default_rolling_policy())
    .build()
)

def split(line):
    yield from line.split()

# compute word count
ds = ds.flat_map(split) \
    .map(lambda i: (i, 1), output_type=Types.TUPLE([Types.STRING(), Types.INT()])) \
    .key_by(lambda i: i[0]) \
    .reduce(lambda i, j: (i[0], i[1] + j[1]))
```

最后一步是执行 PyFlink DataStream API 作业。PyFlink applications 是懒加载的，并且只有在完全构建之后才会提交给集群上执行。要执行一个应用程序，你只需简单地调用 `env.execute()`。

```python
env.execute()
```

完整的代码如下:

```python
import argparse
import logging
import sys

from pyflink.common import WatermarkStrategy, Encoder, Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors import (FileSource, StreamFormat, FileSink, OutputFileConfig,
                                           RollingPolicy)


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
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.BATCH)
    # write all the data to one file
    env.set_parallelism(1)

    # define the source
    if input_path is not None:
        ds = env.from_source(
            source=FileSource.for_record_stream_format(StreamFormat.text_line_format(),
                                                       input_path)
                             .process_static_file_set().build(),
            watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
            source_name="file_source"
        )
    else:
        print("Executing word_count example with default input data set.")
        print("Use --input to specify file input.")
        ds = env.from_collection(word_count_data)

    def split(line):
        yield from line.split()

    # compute word count
    ds = ds.flat_map(split) \
           .map(lambda i: (i, 1), output_type=Types.TUPLE([Types.STRING(), Types.INT()])) \
           .key_by(lambda i: i[0]) \
           .reduce(lambda i, j: (i[0], i[1] + j[1]))

    # define the sink
    if output_path is not None:
        ds.sink_to(
            sink=FileSink.for_row_format(
                base_path=output_path,
                encoder=Encoder.simple_string_encoder())
            .with_output_file_config(
                OutputFileConfig.builder()
                .with_part_prefix("prefix")
                .with_part_suffix(".ext")
                .build())
            .with_rolling_policy(RollingPolicy.default_rolling_policy())
            .build()
        )
    else:
        print("Printing result to stdout. Use --output to specify output path.")
        ds.print()

    # submit for execution
    env.execute()


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

## 执行一个 Flink Python DataStream API 程序

现在你已经编写好 PyFlink 程序，可以通过如下命令执行它:

```bash
$ python word_count.py
```

这个命令会在本地集群中构建并运行 PyFlink 程序。你也可以使用 [Job Submission Examples]({{< ref "docs/deployment/cli" >}}#submitting-pyflink-jobs) 中描述的命令将其提交到远程集群。

最后，你可以得到如下运行结果:

```bash
(a,5)
(Be,1)
(Is,1)
(No,2)
...
```

本教程为你开始编写自己的 PyFlink DataStream API 程序提供了基础。你也可以访问 {{< gh_link file="flink-python/pyflink/examples" name="PyFlink 示例" >}}，了解更多关于 PyFlink 的示例。
如果需要了解更多关于 Python DataStream API 的使用，请查阅 {{< pythondoc name="Flink Python API 文档">}}。
