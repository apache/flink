---
title: "DataStream API Tutorial"
weight: 22
type: docs
aliases:
  - /dev/python/datastream_tutorial.html
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

# DataStream API Tutorial

Apache Flink offers a DataStream API for building robust, stateful streaming applications. It provides fine-grained control over state and time, which allows for the implementation of advanced event-driven systems. In this step-by-step guide, you’ll learn how to build a simple streaming application with PyFlink and the DataStream API.

## What Will You Be Building? 

In this tutorial, you will learn how to write a simple Python DataStream pipeline.
The pipeline will read data from a csv file, compute the word frequency and write the results to an output file.

## Prerequisites

This walkthrough assumes that you have some familiarity with Python, but you should be able to follow along even if you come from a different programming language.

## Help, I’m Stuck! 

If you get stuck, check out the [community support resources](https://flink.apache.org/community.html).
In particular, Apache Flink's [user mailing list](https://flink.apache.org/community.html#mailing-lists) consistently ranks as one of the most active of any Apache project and a great way to get help quickly. 

## How To Follow Along

If you want to follow along, you will require a computer with: 

* Java 11
* Python 3.8, 3.9, 3.10 or 3.11

Using Python DataStream API requires installing PyFlink, which is available on [PyPI](https://pypi.org/project/apache-flink/) and can be easily installed using `pip`. 

```bash
$ python -m pip install apache-flink
```

Once PyFlink is installed, you can move on to write a Python DataStream job.

## Writing a Flink Python DataStream API Program

DataStream API applications begin by declaring an execution environment (`StreamExecutionEnvironment`), the context in which a streaming program is executed. This is what you will use to set the properties of your job (e.g. default parallelism, restart strategy), create your sources and finally trigger the execution of the job.

```python
env = StreamExecutionEnvironment.get_execution_environment()
env.set_runtime_mode(RuntimeExecutionMode.BATCH)
env.set_parallelism(1)
```

Once a `StreamExecutionEnvironment` is created, you can use it to declare your _source_. Sources ingest data from external systems, such as Apache Kafka, Rabbit MQ, or Apache Pulsar, into Flink Jobs. 

To keep things simple, this walkthrough uses a source which reads data from a file.

```python
ds = env.from_source(
    source=FileSource.for_record_stream_format(StreamFormat.text_line_format(),
                                               input_path)
                     .process_static_file_set().build(),
    watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
    source_name="file_source"
)
```

You can now perform transformations on this data stream, or just write the data to an external system using a _sink_. This walkthrough uses the `FileSink` sink connector to write the data into a file.

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

The last step is to execute the actual PyFlink DataStream API job. PyFlink applications are built lazily and shipped to the cluster for execution only once fully formed. To execute an application, you simply call `env.execute()`.

```python
env.execute()
```

The complete code so far:

```python
import argparse
import logging
import sys

from pyflink.common import WatermarkStrategy, Encoder, Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.file_system import FileSource, StreamFormat, FileSink, OutputFileConfig, RollingPolicy


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

## Executing a Flink Python DataStream API Program

Now that you defined your PyFlink program, you can run the example you just created on the command line:

```bash
$ python word_count.py
```

The command builds and runs your PyFlink program in a local mini cluster. You can alternatively submit it to a remote cluster using the instructions detailed in [Job Submission Examples]({{< ref "docs/deployment/cli" >}}#submitting-pyflink-jobs).

Finally, you can see the execution results similar to the following:

```bash
(a,5)
(Be,1)
(Is,1)
(No,2)
...
```

This walkthrough gives you the foundations to get started writing your own PyFlink DataStream API programs.
You can also refer to {{< gh_link file="flink-python/pyflink/examples" name="PyFlink Examples" >}} for more examples.
To learn more about the Python DataStream API, please refer to {{< pythondoc name="Flink Python API Docs">}} for more details.
