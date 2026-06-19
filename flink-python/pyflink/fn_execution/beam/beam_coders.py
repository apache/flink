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
from apache_beam.coders import Coder
from apache_beam.coders.coders import FastCoder, LengthPrefixCoder
from apache_beam.portability import common_urns
from apache_beam.typehints import typehints

from pyflink import fn_execution
from pyflink.fn_execution.coders import LengthPrefixBaseCoder
from pyflink.fn_execution.flink_fn_execution_pb2 import CoderInfoDescriptor

if fn_execution.PYFLINK_CYTHON_ENABLED:
    from pyflink.fn_execution.beam import beam_coder_impl_fast as beam_coder_impl
    from pyflink.fn_execution.beam.beam_coder_impl_fast import FlinkFieldCoderBeamWrapper
    from pyflink.fn_execution.beam.beam_coder_impl_fast import FlinkLengthPrefixCoderBeamWrapper
else:
    from pyflink.fn_execution.beam import beam_coder_impl_slow as beam_coder_impl
    from pyflink.fn_execution.beam.beam_coder_impl_slow import FlinkFieldCoderBeamWrapper
    from pyflink.fn_execution.beam.beam_coder_impl_slow import FlinkLengthPrefixCoderBeamWrapper

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
            return FlinkLengthPrefixCoderBeamWrapper(self._create_impl())
        else:
            return FlinkFieldCoderBeamWrapper(self._create_impl())

    def to_type_hint(self):
        return typehints.Any

    @Coder.register_urn(FLINK_CODER_URN, CoderInfoDescriptor)
    def _pickle_from_runner_api_parameter(
            coder_info_descriptor_proto, unused_components, unused_context):
        return FlinkCoder(LengthPrefixBaseCoder.from_coder_info_descriptor_proto(
            coder_info_descriptor_proto))

    def __repr__(self):
        return 'FlinkCoder[%s]' % repr(self._internal_coder)

    def __eq__(self, other: 'FlinkCoder'):
        return (self.__class__ == other.__class__
                and self._internal_coder == other._internal_coder)

    def __ne__(self, other):
        return not self == other

    def __hash__(self):
        return hash(self._internal_coder)
