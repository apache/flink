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

from abc import abstractmethod, ABC
from io import BytesIO
from typing import TypeVar, Generic

T = TypeVar('T')

__all__ = ['TypeSerializer']


class TypeSerializer(ABC, Generic[T]):
    """
    This interface describes the methods that are required for a data type to be handled by the
    Flink runtime. Specifically, this interface contains the serialization and deserialization
    methods.
    """

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        return "%s()" % self.__class__.__name__

    def __hash__(self):
        return hash(str(self))

    @abstractmethod
    def serialize(self, element: T, stream: BytesIO) -> None:
        """
        Serializes an element to the output stream.
        """
        pass

    @abstractmethod
    def deserialize(self, stream: BytesIO) -> T:
        """
        Returns a deserialized element from the input stream.
        """
        pass

    def _get_coder(self):
        serialize_func = self.serialize
        deserialize_func = self.deserialize

        class CoderAdapter(object):

            def encode_nested(self, element):
                bytes_io = BytesIO()
                serialize_func(element, bytes_io)
                return bytes_io.getvalue()

            def decode_nested(self, bytes_data):
                bytes_io = BytesIO(bytes_data)
                return deserialize_func(bytes_io)

        return CoderAdapter()


void = b''


class VoidNamespaceSerializer(TypeSerializer[bytes]):

    def serialize(self, element: bytes, stream: BytesIO) -> None:
        pass

    def deserialize(self, stream: BytesIO) -> bytes:
        return void
