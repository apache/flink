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
from abc import ABC, abstractmethod
from enum import Enum
from typing import Union, Optional

from py4j.java_gateway import JavaObject

from pyflink.datastream.functions import JavaFunctionWrapper
from pyflink.java_gateway import get_gateway


class Source(JavaFunctionWrapper):
    """
    Base class for all unified data source in Flink.
    """

    def __init__(self, source: Union[str, JavaObject]):
        """
        Constructor of Source.

        :param source: The java Source object.
        """
        super(Source, self).__init__(source)


class Sink(JavaFunctionWrapper):
    """
    Base class for all unified data sink in Flink.
    """

    def __init__(self, sink: Union[str, JavaObject]):
        """
        Constructor of Sink.

        :param sink: The java Sink object.
        """
        super(Sink, self).__init__(sink)


class DeliveryGuarantee(Enum):
    """
    DeliverGuarantees that can be chosen. In general your pipeline can only offer the lowest
    delivery guarantee which is supported by your sources and sinks.

    :data: `EXACTLY_ONCE`:

    Records are only delivered exactly-once also under failover scenarios. To build a complete
    exactly-once pipeline is required that the source and sink support exactly-once and are
    properly configured.

    :data: `AT_LEAST_ONCE`:

    Records are ensured to be delivered but it may happen that the same record is delivered
    multiple times. Usually, this guarantee is faster than the exactly-once delivery.

    :data: `NONE`:

    Records are delivered on a best effort basis. It is often the fastest way to process records
    but it may happen that records are lost or duplicated.
    """

    EXACTLY_ONCE = 0,
    AT_LEAST_ONCE = 1,
    NONE = 2

    def _to_j_delivery_guarantee(self):
        JDeliveryGuarantee = get_gateway().jvm \
            .org.apache.flink.connector.base.DeliveryGuarantee
        return getattr(JDeliveryGuarantee, self.name)


class StreamTransformer(ABC):

    @abstractmethod
    def apply(self, ds):
        pass


class SupportsPreprocessing(ABC):

    @abstractmethod
    def get_transformer(self) -> Optional[StreamTransformer]:
        pass
