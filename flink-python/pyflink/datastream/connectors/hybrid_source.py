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
from py4j.java_gateway import JavaObject

from pyflink.datastream.connectors import Source
from pyflink.java_gateway import get_gateway


__all__ = [
    'HybridSource',
    'HybridSourceBuilder'
]


class HybridSource(Source):
    """
    Hybrid source that switches underlying sources based on configured source chain.

    A simple example with FileSource and KafkaSource with fixed Kafka start position:

    ::

        >>> file_source = FileSource \\
        ...      .for_record_stream_format(StreamFormat.text_line_format(), test_dir) \\
        ...      .build()
        >>> kafka_source = KafkaSource \\
        ...     .builder() \\
        ...     .set_bootstrap_servers('localhost:9092') \\
        ...     .set_group_id('MY_GROUP') \\
        ...     .set_topics('quickstart-events') \\
        ...     .set_value_only_deserializer(SimpleStringSchema()) \\
        ...     .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \\
        ...     .build()
        >>> hybrid_source = HybridSource.builder(file_source).add_source(kafka_source).build()
    """

    def __init__(self, j_hybrid_source: JavaObject):
        super(HybridSource, self).__init__(j_hybrid_source)

    @staticmethod
    def builder(first_source: Source) -> 'HybridSourceBuilder':
        JHybridSource = get_gateway().jvm.org.apache.flink.connector.base.source.hybrid.HybridSource
        return HybridSourceBuilder(JHybridSource.builder(first_source.get_java_function()))


class HybridSourceBuilder(object):

    def __init__(self, j_hybrid_source_builder):
        self._j_hybrid_source_builder = j_hybrid_source_builder

    def add_source(self, source: Source) -> 'HybridSourceBuilder':
        self._j_hybrid_source_builder.addSource(source.get_java_function())
        return self

    def build(self) -> 'HybridSource':
        return HybridSource(self._j_hybrid_source_builder.build())
