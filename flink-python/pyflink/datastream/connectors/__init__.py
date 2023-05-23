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
from pyflink.datastream.connectors.base import Sink, Source, DeliveryGuarantee


__all__ = [
    'Sink',
    'Source',
    'DeliveryGuarantee'
]


def _install():
    from pyflink.datastream import connectors

    # number_seq
    from pyflink.datastream.connectors import number_seq
    setattr(connectors, 'NumberSequenceSource', number_seq.NumberSequenceSource)

    # jdbc
    from pyflink.datastream.connectors import jdbc
    setattr(connectors, 'JdbcSink', jdbc.JdbcSink)
    setattr(connectors, 'JdbcConnectionOptions', jdbc.JdbcConnectionOptions)
    setattr(connectors, 'JdbcExecutionOptions', jdbc.JdbcExecutionOptions)

    # kafka
    from pyflink.datastream.connectors import kafka
    setattr(connectors, 'KafkaSource', kafka.KafkaSource)
    setattr(connectors, 'FlinkKafkaConsumer', kafka.FlinkKafkaConsumer)
    setattr(connectors, 'FlinkKafkaProducer', kafka.FlinkKafkaProducer)
    setattr(connectors, 'Semantic', kafka.Semantic)

    # pulsar
    from pyflink.datastream.connectors import pulsar
    setattr(connectors, 'PulsarSource', pulsar.PulsarSource)
    setattr(connectors, 'PulsarSourceBuilder', pulsar.PulsarSourceBuilder)
    setattr(connectors, 'StartCursor', pulsar.StartCursor)
    setattr(connectors, 'StopCursor', pulsar.StopCursor)

    # rabbitmq
    from pyflink.datastream.connectors import rabbitmq
    setattr(connectors, 'RMQSource', rabbitmq.RMQSource)
    setattr(connectors, 'RMQSink', rabbitmq.RMQSink)
    setattr(connectors, 'RMQConnectionConfig', rabbitmq.RMQConnectionConfig)

    # filesystem
    from pyflink.datastream.connectors import file_system
    setattr(connectors, 'BucketAssigner', file_system.BucketAssigner)
    setattr(connectors, 'FileEnumeratorProvider', file_system.FileEnumeratorProvider)
    setattr(connectors, 'FileSink', file_system.FileSink)
    setattr(connectors, 'FileSplitAssignerProvider', file_system.FileSplitAssignerProvider)
    setattr(connectors, 'FileSource', file_system.FileSource)
    setattr(connectors, 'FileSourceBuilder', file_system.FileSourceBuilder)
    setattr(connectors, 'OutputFileConfig', file_system.OutputFileConfig)
    setattr(connectors, 'RollingPolicy', file_system.RollingPolicy)
    setattr(connectors, 'StreamFormat', file_system.StreamFormat)
    setattr(connectors, 'StreamingFileSink', file_system.StreamingFileSink)


# for backward compatibility
_install()
del _install
