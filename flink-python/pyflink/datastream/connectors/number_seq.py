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
from pyflink.datastream.connectors import Source
from pyflink.java_gateway import get_gateway

__all__ = [
    'NumberSequenceSource'
]


class NumberSequenceSource(Source):
    """
    A data source that produces a sequence of numbers (longs). This source is useful for testing and
    for cases that just need a stream of N events of any kind.

    The source splits the sequence into as many parallel sub-sequences as there are parallel
    source readers. Each sub-sequence will be produced in order. Consequently, if the parallelism is
    limited to one, this will produce one sequence in order.

    This source is always bounded. For very long sequences (for example over the entire domain of
    long integer values), user may want to consider executing the application in a streaming manner,
    because, despite the fact that the produced stream is bounded, the end bound is pretty far away.
    """

    def __init__(self, start: int, end: int):
        """
        Creates a new NumberSequenceSource that produces parallel sequences covering the
        range start to end (both boundaries are inclusive).
        """
        JNumberSequenceSource = get_gateway().jvm.org.apache.flink.api.connector.source.lib.\
            NumberSequenceSource
        j_seq_source = JNumberSequenceSource(start, end)
        super(NumberSequenceSource, self).__init__(source=j_seq_source)
