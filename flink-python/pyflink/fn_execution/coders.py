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
import sys

from apache_beam.coders import Coder, VarIntCoder
from apache_beam.coders.coders import FastCoder

from pyflink.fn_execution import coder_impl
from pyflink.fn_execution import flink_fn_execution_pb2

FLINK_SCHEMA_CODER_URN = "flink:coder:schema:v1"

if sys.version > '3':
    xrange = range


__all__ = ['RowCoder']


class RowCoder(FastCoder):
    """
    Coder for Row.
    """

    def __init__(self, field_coders):
        self._field_coders = field_coders

    def _create_impl(self):
        return coder_impl.RowCoderImpl([c.get_impl() for c in self._field_coders])

    def is_deterministic(self):
        return all(c.is_deterministic() for c in self._field_coders)

    def to_type_hint(self):
        from pyflink.table import Row
        return Row

    def __repr__(self):
        return 'RowCoder[%s]' % ', '.join(str(c) for c in self._field_coders)

    def __eq__(self, other):
        return (self.__class__ == other.__class__
                and len(self._field_coders) == len(other._field_coders)
                and [self._field_coders[i] == other._field_coders[i] for i in
                     xrange(len(self._field_coders))])

    def __ne__(self, other):
        return not self == other

    def __hash__(self):
        return hash(self._field_coders)


@Coder.register_urn(FLINK_SCHEMA_CODER_URN, flink_fn_execution_pb2.Schema)
def _pickle_from_runner_api_parameter(schema_proto, unused_components, unused_context):
    return RowCoder([from_proto(f.type) for f in schema_proto.fields])


def from_proto(field_type):
    """
    Creates the corresponding :class:`Coder` given the protocol representation of the field type.

    :param field_type: the protocol representation of the field type
    :return: :class:`Coder`
    """
    if field_type.type_name == flink_fn_execution_pb2.Schema.TypeName.BIGINT:
        return VarIntCoder()
    elif field_type.type_name == flink_fn_execution_pb2.Schema.TypeName.ROW:
        return RowCoder([from_proto(f.type) for f in field_type.row_schema.fields])
    else:
        raise ValueError("field_type %s is not supported." % field_type)
