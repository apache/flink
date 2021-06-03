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
import pickle
from typing import Any

from apache_beam.coders import Coder, coder_impl
from apache_beam.coders.coders import FastCoder, LengthPrefixCoder
from apache_beam.portability import common_urns
from apache_beam.typehints import typehints

from pyflink.fn_execution.coders import ArrowCoder, OverWindowArrowCoder, LengthPrefixBaseCoder
from pyflink.fn_execution.flink_fn_execution_pb2 import CoderParam

try:
    from pyflink.fn_execution.beam import beam_coder_impl_fast as beam_coder_impl
    from pyflink.fn_execution.beam.beam_coder_impl_fast import PassThroughPrefixCoderImpl
except ImportError:
    from pyflink.fn_execution.beam import beam_coder_impl_slow as beam_coder_impl
    PassThroughPrefixCoderImpl = beam_coder_impl.BeamCoderImpl

FLINK_CODER_URN = "flink:coder:v1"


class PassThroughLengthPrefixCoder(LengthPrefixCoder):
    """
    Coder which doesn't prefix the length of the encoded object as the length prefix will be handled
    by the wrapped value coder.
    """

    def __init__(self, value_coder):
        super(PassThroughLengthPrefixCoder, self).__init__(value_coder)

    def _create_impl(self):
        return beam_coder_impl.PassThroughLengthPrefixCoderImpl(self._value_coder.get_impl())

    def __repr__(self):
        return 'PassThroughLengthPrefixCoder[%s]' % self._value_coder


Coder.register_structured_urn(
    common_urns.coders.LENGTH_PREFIX.urn, PassThroughLengthPrefixCoder)


class FlinkCoder(FastCoder):

    def __init__(self, internal_coder):
        self._internal_coder = internal_coder

    def _create_impl(self):
        return self._internal_coder.get_impl()

    def get_impl(self):
        if isinstance(self._internal_coder, LengthPrefixBaseCoder):
            if isinstance(self._internal_coder._field_coder, (ArrowCoder, OverWindowArrowCoder)):
                from pyflink.fn_execution.beam.beam_coder_impl_slow import BeamCoderImpl
                return BeamCoderImpl(self._create_impl())
            else:
                return beam_coder_impl.BeamCoderImpl(self._create_impl())
        else:
            return PassThroughPrefixCoderImpl(self._create_impl())

    def to_type_hint(self):
        return typehints.Any

    @Coder.register_urn(FLINK_CODER_URN, CoderParam)
    def _pickle_from_runner_api_parameter(coder_praram_proto, unused_components, unused_context):
        return FlinkCoder(LengthPrefixBaseCoder.from_coder_param_proto(coder_praram_proto))

    def __repr__(self):
        return 'FlinkCoder[%s]' % repr(self._internal_coder)

    def __eq__(self, other: 'FlinkCoder'):
        return (self.__class__ == other.__class__
                and self._internal_coder == other._internal_coder)

    def __ne__(self, other):
        return not self == other

    def __hash__(self):
        return hash(self._internal_coder)


class DataViewFilterCoder(FastCoder):

    def to_type_hint(self):
        return Any

    def __init__(self, udf_data_view_specs):
        self._udf_data_view_specs = udf_data_view_specs

    def filter_data_views(self, row):
        i = 0
        for specs in self._udf_data_view_specs:
            for spec in specs:
                row[i][spec.field_index] = None
            i += 1
        return row

    def _create_impl(self):
        filter_data_views = self.filter_data_views
        dumps = pickle.dumps
        HIGHEST_PROTOCOL = pickle.HIGHEST_PROTOCOL
        return coder_impl.CallbackCoderImpl(
            lambda x: dumps(filter_data_views(x), HIGHEST_PROTOCOL), pickle.loads)
