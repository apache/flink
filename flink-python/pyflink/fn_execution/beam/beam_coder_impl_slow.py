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
from typing import Any

from apache_beam.coders.coder_impl import StreamCoderImpl, create_InputStream, create_OutputStream

from pyflink.fn_execution.stream_slow import OutputStream
from pyflink.fn_execution.beam.beam_stream_slow import BeamInputStream, BeamTimeBasedOutputStream


class PassThroughLengthPrefixCoderImpl(StreamCoderImpl):
    def __init__(self, value_coder):
        self._value_coder = value_coder

    def encode_to_stream(self, value, out: create_OutputStream, nested: bool) -> Any:
        self._value_coder.encode_to_stream(value, out, nested)

    def decode_from_stream(self, in_stream: create_InputStream, nested: bool) -> Any:
        return self._value_coder.decode_from_stream(in_stream, nested)

    def get_estimated_size_and_observables(self, value: Any, nested=False):
        return 0, []

    def __repr__(self):
        return 'PassThroughLengthPrefixCoderImpl[%s]' % self._value_coder


class FlinkFieldCoderBeamWrapper(StreamCoderImpl):
    """
    Bridge between Beam coder and Flink coder for the low-level FieldCoder.
    """
    def __init__(self, value_coder):
        self._value_coder = value_coder
        self._data_output_stream = OutputStream()

    def encode_to_stream(self, value, out_stream: create_OutputStream, nested):
        self._value_coder.encode_to_stream(value, self._data_output_stream)
        out_stream.write(self._data_output_stream.get())
        self._data_output_stream.clear()

    def decode_from_stream(self, in_stream: create_InputStream, nested):
        data_input_stream = BeamInputStream(in_stream)
        return self._value_coder.decode_from_stream(data_input_stream)

    def __repr__(self):
        return 'FlinkFieldCoderBeamWrapper[%s]' % self._value_coder


class FlinkLengthPrefixCoderBeamWrapper(FlinkFieldCoderBeamWrapper):
    """
    Bridge between Beam coder and Flink coder for the top-level LengthPrefixCoder.
    """
    def __init__(self, value_coder):
        super(FlinkLengthPrefixCoderBeamWrapper, self).__init__(value_coder)
        self._output_stream = BeamTimeBasedOutputStream()

    def encode_to_stream(self, value, out_stream: create_OutputStream, nested):
        self._output_stream.reset_output_stream(out_stream)

        self._value_coder.encode_to_stream(value, self._data_output_stream)
        self._output_stream.write(self._data_output_stream.get())
        self._data_output_stream.clear()

    def __repr__(self):
        return 'FlinkLengthPrefixCoderBeamWrapper[%s]' % self._value_coder
