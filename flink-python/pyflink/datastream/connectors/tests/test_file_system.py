################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

from pyflink.common import Duration
from pyflink.common.serialization import Encoder
from pyflink.common.typeinfo import Types
from pyflink.datastream.connectors.file_system import FileCompactStrategy, FileCompactor, \
    StreamingFileSink, OutputFileConfig, FileSource, StreamFormat, FileEnumeratorProvider, \
    FileSplitAssignerProvider, RollingPolicy, FileSink, BucketAssigner

from pyflink.testing.test_case_utils import PyFlinkStreamingTestCase
from pyflink.util.java_utils import load_java_class


class FileSystemTests(PyFlinkStreamingTestCase):

    def test_stream_file_sink(self):
        self.env.set_parallelism(2)
        ds = self.env.from_collection([('ab', 1), ('bdc', 2), ('cfgs', 3), ('deeefg', 4)],
                                      type_info=Types.ROW([Types.STRING(), Types.INT()]))
        ds.map(
            lambda a: a[0],
            Types.STRING()).add_sink(
            StreamingFileSink.for_row_format(self.tempdir, Encoder.simple_string_encoder())
                .with_rolling_policy(
                    RollingPolicy.default_rolling_policy(
                        part_size=1024 * 1024 * 1024,
                        rollover_interval=15 * 60 * 1000,
                        inactivity_interval=5 * 60 * 1000))
                .with_output_file_config(
                    OutputFileConfig.OutputFileConfigBuilder()
                    .with_part_prefix("prefix")
                    .with_part_suffix("suffix").build()).build())

        self.env.execute("test_streaming_file_sink")

        results = []
        import os
        for root, dirs, files in os.walk(self.tempdir, topdown=True):
            for file in files:
                self.assertTrue(file.startswith('.prefix'))
                self.assertTrue('suffix' in file)
                path = root + "/" + file
                with open(path) as infile:
                    for line in infile:
                        results.append(line)

        expected = ['deeefg\n', 'bdc\n', 'ab\n', 'cfgs\n']
        results.sort()
        expected.sort()
        self.assertEqual(expected, results)

    def test_file_source(self):
        stream_format = StreamFormat.text_line_format()
        paths = ["/tmp/1.txt", "/tmp/2.txt"]
        file_source_builder = FileSource.for_record_stream_format(stream_format, *paths)
        file_source = file_source_builder\
            .monitor_continuously(Duration.of_days(1)) \
            .set_file_enumerator(FileEnumeratorProvider.default_splittable_file_enumerator()) \
            .set_split_assigner(FileSplitAssignerProvider.locality_aware_split_assigner()) \
            .build()

        continuous_setting = file_source.get_java_function().getContinuousEnumerationSettings()
        self.assertIsNotNone(continuous_setting)
        self.assertEqual(Duration.of_days(1), Duration(continuous_setting.getDiscoveryInterval()))

        input_paths_field = \
            load_java_class("org.apache.flink.connector.file.src.AbstractFileSource"). \
            getDeclaredField("inputPaths")
        input_paths_field.setAccessible(True)
        input_paths = input_paths_field.get(file_source.get_java_function())
        self.assertEqual(len(input_paths), len(paths))
        self.assertEqual(str(input_paths[0]), paths[0])
        self.assertEqual(str(input_paths[1]), paths[1])

    def test_file_sink(self):
        base_path = "/tmp/1.txt"
        encoder = Encoder.simple_string_encoder()
        file_sink_builder = FileSink.for_row_format(base_path, encoder)
        file_sink = file_sink_builder\
            .with_bucket_check_interval(1000) \
            .with_bucket_assigner(BucketAssigner.base_path_bucket_assigner()) \
            .with_rolling_policy(RollingPolicy.on_checkpoint_rolling_policy()) \
            .with_output_file_config(
                OutputFileConfig.builder().with_part_prefix("pre").with_part_suffix("suf").build())\
            .enable_compact(FileCompactStrategy.builder()
                            .enable_compaction_on_checkpoint(3)
                            .set_size_threshold(1024)
                            .set_num_compact_threads(2)
                            .build(),
                            FileCompactor.concat_file_compactor(b'\n')) \
            .build()

        buckets_builder_field = \
            load_java_class("org.apache.flink.connector.file.sink.FileSink"). \
            getDeclaredField("bucketsBuilder")
        buckets_builder_field.setAccessible(True)
        buckets_builder = buckets_builder_field.get(file_sink.get_java_function())

        self.assertEqual("DefaultRowFormatBuilder", buckets_builder.getClass().getSimpleName())

        row_format_builder_clz = load_java_class(
            "org.apache.flink.connector.file.sink.FileSink$RowFormatBuilder")
        encoder_field = row_format_builder_clz.getDeclaredField("encoder")
        encoder_field.setAccessible(True)
        self.assertEqual("SimpleStringEncoder",
                         encoder_field.get(buckets_builder).getClass().getSimpleName())

        interval_field = row_format_builder_clz.getDeclaredField("bucketCheckInterval")
        interval_field.setAccessible(True)
        self.assertEqual(1000, interval_field.get(buckets_builder))

        bucket_assigner_field = row_format_builder_clz.getDeclaredField("bucketAssigner")
        bucket_assigner_field.setAccessible(True)
        self.assertEqual("BasePathBucketAssigner",
                         bucket_assigner_field.get(buckets_builder).getClass().getSimpleName())

        rolling_policy_field = row_format_builder_clz.getDeclaredField("rollingPolicy")
        rolling_policy_field.setAccessible(True)
        self.assertEqual("OnCheckpointRollingPolicy",
                         rolling_policy_field.get(buckets_builder).getClass().getSimpleName())

        output_file_config_field = row_format_builder_clz.getDeclaredField("outputFileConfig")
        output_file_config_field.setAccessible(True)
        output_file_config = output_file_config_field.get(buckets_builder)
        self.assertEqual("pre", output_file_config.getPartPrefix())
        self.assertEqual("suf", output_file_config.getPartSuffix())

        compact_strategy_field = row_format_builder_clz.getDeclaredField("compactStrategy")
        compact_strategy_field.setAccessible(True)
        compact_strategy = compact_strategy_field.get(buckets_builder)
        self.assertEqual(3, compact_strategy.getNumCheckpointsBeforeCompaction())
        self.assertEqual(1024, compact_strategy.getSizeThreshold())
        self.assertEqual(2, compact_strategy.getNumCompactThreads())

        file_compactor_field = row_format_builder_clz.getDeclaredField("fileCompactor")
        file_compactor_field.setAccessible(True)
        file_compactor = file_compactor_field.get(buckets_builder)
        self.assertEqual("ConcatFileCompactor", file_compactor.getClass().getSimpleName())
        concat_file_compactor_clz = load_java_class(
            "org.apache.flink.connector.file.sink.compactor.ConcatFileCompactor"
        )
        file_delimiter_field = concat_file_compactor_clz.getDeclaredField("fileDelimiter")
        file_delimiter_field.setAccessible(True)
        file_delimiter = file_delimiter_field.get(file_compactor)
        self.assertEqual(b'\n', file_delimiter)
