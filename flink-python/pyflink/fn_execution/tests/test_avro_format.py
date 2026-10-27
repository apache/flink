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
"""Tests for the JVM-compatible Avro encoder and decoder."""
import io
import logging
import unittest
from decimal import Decimal

import avro.schema

from pyflink.fn_execution.formats.avro import (
    FlinkAvroDatumReader,
    FlinkAvroDatumWriter,
    FlinkAvroDecoder,
    FlinkAvroEncoder,
)
from pyflink.testing.test_case_utils import PyFlinkTestCase

DECIMAL_THEN_INT = """
{
  "type": "record",
  "name": "DecimalRecord",
  "fields": [
    {
      "name": "amount",
      "type": {"type": "bytes", "logicalType": "decimal", "precision": 8, "scale": 2}
    },
    {"name": "tail", "type": "int"}
  ]
}
"""


class AvroFormatTests(PyFlinkTestCase):

    @staticmethod
    def _encode(schema, record):
        buffer = io.BytesIO()
        FlinkAvroDatumWriter(schema).write(record, FlinkAvroEncoder(buffer))
        return buffer.getvalue()

    def test_decimal_bytes_are_framed_like_any_other_bytes_field(self):
        schema = avro.schema.parse(DECIMAL_THEN_INT)
        encoded = self._encode(schema, {"amount": Decimal("12.34"), "tail": 7})

        # 4-byte length, the two's-complement unscaled value, then the next field. Sizing the
        # payload as an 8-byte long instead shifts every following field.
        self.assertEqual("0000000204d200000007", encoded.hex())

    def test_decimal_bytes_are_readable_field_by_field(self):
        schema = avro.schema.parse(DECIMAL_THEN_INT)
        encoded = self._encode(schema, {"amount": Decimal("12.34"), "tail": 7})

        decoder = FlinkAvroDecoder(io.BytesIO(encoded))
        self.assertEqual(b"\x04\xd2", decoder.read_bytes())
        self.assertEqual(7, decoder.read_int())

    def test_decimal_payload_is_unchanged_two_s_complement(self):
        schema = avro.schema.parse(DECIMAL_THEN_INT)
        expected = {
            "0.00": "00",
            "12.34": "04d2",
            "-12.34": "fb2e",
            "1.28": "0080",
            "-1.28": "ff80",
            "-1.29": "ff7f",
        }
        for value, payload in expected.items():
            with self.subTest(value=value):
                encoded = self._encode(schema, {"amount": Decimal(value), "tail": 0})
                length = len(bytes.fromhex(payload))
                self.assertEqual(length.to_bytes(4, "big").hex(), encoded[:4].hex())
                self.assertEqual(payload, encoded[4:4 + length].hex())

    def test_decimal_written_by_the_old_framing_is_still_readable(self):
        schema = avro.schema.parse(DECIMAL_THEN_INT)
        record = {"amount": Decimal("12.34"), "tail": 7}

        # What an earlier version wrote: the payload length as a fixed 8-byte long. Kept readable
        # because Python state may still hold it.
        legacy = bytes.fromhex("000000000000000204d200000007")
        decoded = FlinkAvroDatumReader(schema, schema).read(
            FlinkAvroDecoder(io.BytesIO(legacy)))
        self.assertEqual(record, decoded)

        # The current framing is read from the same method, so neither shape needs a flag.
        current = self._encode(schema, record)
        self.assertEqual("0000000204d200000007", current.hex())
        self.assertEqual(
            record,
            FlinkAvroDatumReader(schema, schema).read(FlinkAvroDecoder(io.BytesIO(current))))

    def test_old_framing_is_readable_for_every_payload_length(self):
        schema = avro.schema.parse(DECIMAL_THEN_INT)
        for value in ("0.00", "12.34", "-12.34", "1.28", "-1.28", "999999.99", "-999999.99"):
            with self.subTest(value=value):
                current = self._encode(schema, {"amount": Decimal(value), "tail": 7})
                size, payload = current[:4], current[4:-4]
                # re-frame the same payload the way the old encoder did
                legacy = int.from_bytes(size, "big").to_bytes(8, "big") + payload + current[-4:]
                decoded = FlinkAvroDatumReader(schema, schema).read(
                    FlinkAvroDecoder(io.BytesIO(legacy)))
                self.assertEqual({"amount": Decimal(value), "tail": 7}, decoded)

    def test_decimal_round_trip(self):
        schema = avro.schema.parse(DECIMAL_THEN_INT)
        for value in ("0.00", "12.34", "-12.34", "1.27", "-1.28", "999999.99", "-999999.99"):
            with self.subTest(value=value):
                record = {"amount": Decimal(value), "tail": 7}
                encoded = self._encode(schema, record)
                decoded = FlinkAvroDatumReader(schema, schema).read(
                    FlinkAvroDecoder(io.BytesIO(encoded)))
                self.assertEqual(record, decoded)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    unittest.main()
