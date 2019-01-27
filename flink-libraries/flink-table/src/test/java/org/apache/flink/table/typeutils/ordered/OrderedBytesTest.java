/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.typeutils.ordered;

import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.table.dataformat.BinaryString;
import org.apache.flink.table.typeutils.SortedMapViewTypeInfo.ByteArrayComparator;
import org.apache.flink.table.typeutils.ordered.OrderedBytes.Order;

import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for OrderedBytes.
 */
public class OrderedBytesTest {

	/**
	 * Testing BigDecimal.
	 */
	@Test
	public void testBigDecimal() throws IOException {
		OrderedBytes orderedBytes = new OrderedBytes();

		// fill in other gaps in Numeric code paths
		final BigDecimal[] vals = {
			BigDecimal.valueOf(Long.MAX_VALUE), BigDecimal.valueOf(Long.MIN_VALUE),
			BigDecimal.valueOf(Double.MAX_VALUE), BigDecimal.valueOf(Double.MIN_VALUE),
			BigDecimal.valueOf(Long.MAX_VALUE).multiply(BigDecimal.valueOf(100))
		};
		final int[] expectedLengths = { 11, 11, 14, 7, 15 };

		/*
		 * assert encoded values match decoded values. encode into target buffer
		 * starting at an offset to detect over/underflow conditions.
		 */
		for (Order ord : new Order[] { Order.ASCENDING, Order.DESCENDING }) {
			for (int i = 0; i < vals.length; i++) {
				ByteArrayOutputStreamWithPos outputStream = new ByteArrayOutputStreamWithPos();
				DataOutputViewStreamWrapper outputView = new DataOutputViewStreamWrapper(outputStream);
				outputView.skipBytesToWrite(1);

				// verify encode
				assertEquals("Surprising return value.",
							expectedLengths[i], orderedBytes.encodeBigDecimal(outputView, vals[i], ord));
				assertEquals("Surprising serialized length.", expectedLengths[i], outputStream.getPosition() - 1);

				// verify decode
				ByteArrayInputStreamWithPos inputStream = new ByteArrayInputStreamWithPos(outputStream.toByteArray());
				DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(inputStream);
				inputView.skipBytesToRead(1);
				BigDecimal decoded = orderedBytes.decodeBigDecimal(inputView, ord);
				assertEquals("Deserialization failed.", 0, vals[i].compareTo(decoded));
				assertEquals("Did not consume enough bytes.", expectedLengths[i], inputStream.getPosition() - 1);
			}
		}

		/*
		 * assert natural sort order is preserved by the codec.
		 */
		for (Order ord : new Order[]{Order.ASCENDING, Order.DESCENDING}) {
			byte[][] encoded = new byte[vals.length][];
			ByteArrayOutputStreamWithPos outputStream = new ByteArrayOutputStreamWithPos();
			DataOutputViewStreamWrapper outputView = new DataOutputViewStreamWrapper(outputStream);
			for (int i = 0; i < vals.length; i++) {
				outputStream.reset();
				orderedBytes.encodeBigDecimal(outputView, vals[i], ord);
				encoded[i] = outputStream.toByteArray();
			}

			Arrays.sort(encoded, new ByteArrayComparator());
			BigDecimal[] sortedVals = Arrays.copyOf(vals, vals.length);
			if (ord == Order.ASCENDING) {
				Arrays.sort(sortedVals);
			} else {
				Arrays.sort(sortedVals, Collections.reverseOrder());
			}

			for (int i = 0; i < sortedVals.length; i++) {
				ByteArrayInputStreamWithPos inputStream = new ByteArrayInputStreamWithPos(encoded[i]);
				DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(inputStream);
				BigDecimal decoded = orderedBytes.decodeBigDecimal(inputView, ord);
				assertTrue(String.format(
					"Encoded representations do not preserve natural order: <%s>, <%s>, %s",
					sortedVals[i], decoded, ord), sortedVals[i].compareTo(decoded) == 0);
			}
		}
	}

	/**
	 * Testing BigInt.
	 */
	@Test
	public void testBigInteger() throws IOException {
		OrderedBytes orderedBytes = new OrderedBytes();

		// fill in other gaps in Numeric code paths
		final BigInteger[] vals = {
			BigInteger.ZERO, BigInteger.valueOf(Long.MAX_VALUE), BigInteger.valueOf(Long.MIN_VALUE),
			BigInteger.valueOf(Long.MAX_VALUE).multiply(BigInteger.valueOf(100)),
			BigInteger.valueOf(Long.MIN_VALUE).multiply(BigInteger.valueOf(100)),
		};
		final int[] expectedLengths = { 5, 12, 12, 13, 13 };

		/*
		 * assert encoded values match decoded values. encode into target buffer
		 * starting at an offset to detect over/underflow conditions.
		 */
		for (Order ord : new Order[] { Order.ASCENDING, Order.DESCENDING }) {
			for (int i = 0; i < vals.length; i++) {
				ByteArrayOutputStreamWithPos outputStream = new ByteArrayOutputStreamWithPos();
				DataOutputViewStreamWrapper outputView = new DataOutputViewStreamWrapper(outputStream);
				outputView.skipBytesToWrite(1);

				// verify encode
				assertEquals("Surprising return value.",
							expectedLengths[i], orderedBytes.encodeBigInteger(outputView, vals[i], ord));
				assertEquals("Surprising serialized length.", expectedLengths[i], outputStream.getPosition() - 1);

				// verify decode
				ByteArrayInputStreamWithPos inputStream = new ByteArrayInputStreamWithPos(outputStream.toByteArray());
				DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(inputStream);
				inputView.skipBytesToRead(1);
				BigInteger decoded = orderedBytes.decodeBigInteger(inputView, ord);
				assertEquals("Deserialization failed.", 0, vals[i].compareTo(decoded));
				assertEquals("Did not consume enough bytes.", expectedLengths[i], inputStream.getPosition() - 1);
			}
		}

		/*
		 * assert natural sort order is preserved by the codec.
		 */
		for (Order ord : new Order[]{Order.ASCENDING, Order.DESCENDING}) {
			byte[][] encoded = new byte[vals.length][];
			ByteArrayOutputStreamWithPos outputStream = new ByteArrayOutputStreamWithPos();
			DataOutputViewStreamWrapper outputView = new DataOutputViewStreamWrapper(outputStream);
			for (int i = 0; i < vals.length; i++) {
				outputStream.reset();
				orderedBytes.encodeBigInteger(outputView, vals[i], ord);
				encoded[i] = outputStream.toByteArray();
			}

			Arrays.sort(encoded, new ByteArrayComparator());
			BigInteger[] sortedVals = Arrays.copyOf(vals, vals.length);
			if (ord == Order.ASCENDING) {
				Arrays.sort(sortedVals);
			} else {
				Arrays.sort(sortedVals, Collections.reverseOrder());
			}

			for (int i = 0; i < sortedVals.length; i++) {
				ByteArrayInputStreamWithPos inputStream = new ByteArrayInputStreamWithPos(encoded[i]);
				DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(inputStream);
				BigInteger decoded = orderedBytes.decodeBigInteger(inputView, ord);
				assertTrue(String.format(
					"Encoded representations do not preserve natural order: <%s>, <%s>, %s",
					sortedVals[i], decoded, ord), sortedVals[i].compareTo(decoded) == 0);
			}
		}
	}

	/**
	 * Test byte encoding.
	 */
	@Test
	public void testByte() throws IOException {
		OrderedBytes orderedBytes = new OrderedBytes();
		Byte[] vals = {Byte.MIN_VALUE, Byte.MIN_VALUE / 2, 0, Byte.MAX_VALUE / 2, Byte.MAX_VALUE};

		/*
		 * assert encoded values match decoded values.
		 */
		for (Order ord : new Order[]{Order.ASCENDING, Order.DESCENDING}) {
			for (int i = 0; i < vals.length; i++) {
				ByteArrayOutputStreamWithPos outputStream = new ByteArrayOutputStreamWithPos();
				DataOutputViewStreamWrapper outputView = new DataOutputViewStreamWrapper(outputStream);
				outputView.skipBytesToWrite(1);

				// verify encode
				assertEquals("Surprising return value.",
							1, orderedBytes.encodeByte(outputView, vals[i], ord));
				assertEquals("Surprising serialized length.", 1, outputStream.getPosition() - 1);

				// verify decode
				ByteArrayInputStreamWithPos inputStream = new ByteArrayInputStreamWithPos(outputStream.toByteArray());
				DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(inputStream);
				inputView.skipBytesToRead(1);
				assertEquals("Deserialization failed.",
							vals[i].byteValue(), orderedBytes.decodeByte(inputView, ord));
				assertEquals("Did not consume enough bytes.", 1, inputStream.getPosition() - 1);
			}
		}

		/*
		 * assert natural sort order is preserved by the codec.
		 */
		for (Order ord : new Order[]{Order.ASCENDING, Order.DESCENDING}) {
			byte[][] encoded = new byte[vals.length][];
			ByteArrayOutputStreamWithPos outputStream = new ByteArrayOutputStreamWithPos();
			DataOutputViewStreamWrapper outputView = new DataOutputViewStreamWrapper(outputStream);
			for (int i = 0; i < vals.length; i++) {
				outputStream.reset();
				orderedBytes.encodeByte(outputView, vals[i], ord);
				encoded[i] = outputStream.toByteArray();
			}

			Arrays.sort(encoded, new ByteArrayComparator());
			Byte[] sortedVals = Arrays.copyOf(vals, vals.length);
			if (ord == Order.ASCENDING) {
				Arrays.sort(sortedVals);
			} else {
				Arrays.sort(sortedVals, Collections.reverseOrder());
			}

			for (int i = 0; i < sortedVals.length; i++) {
				ByteArrayInputStreamWithPos inputStream = new ByteArrayInputStreamWithPos(encoded[i]);
				DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(inputStream);
				int decoded = orderedBytes.decodeByte(inputView, ord);
				assertEquals(
					String.format(
						"Encoded representations do not preserve natural order: <%s>, <%s>, %s",
						sortedVals[i], decoded, ord),
					sortedVals[i].byteValue(), decoded);
			}
		}
	}

	/**
	 * Test short encoding.
	 */
	@Test
	public void testShort() throws IOException {
		OrderedBytes orderedBytes = new OrderedBytes();
		Short[] vals = {Short.MIN_VALUE, Short.MIN_VALUE / 2, 0, Short.MAX_VALUE / 2, Short.MAX_VALUE};

		/*
		 * assert encoded values match decoded values.
		 */
		for (Order ord : new Order[]{Order.ASCENDING, Order.DESCENDING}) {
			for (int i = 0; i < vals.length; i++) {
				ByteArrayOutputStreamWithPos outputStream = new ByteArrayOutputStreamWithPos();
				DataOutputViewStreamWrapper outputView = new DataOutputViewStreamWrapper(outputStream);
				outputView.skipBytesToWrite(1);

				// verify encode
				assertEquals("Surprising return value.",
							2, orderedBytes.encodeShort(outputView, vals[i], ord));
				assertEquals("Surprising serialized length.",
							2, outputStream.getPosition() - 1);

				// verify decode
				ByteArrayInputStreamWithPos inputStream = new ByteArrayInputStreamWithPos(outputStream.toByteArray());
				DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(inputStream);
				inputView.skipBytesToRead(1);
				assertEquals("Deserialization failed.",
							vals[i].shortValue(), orderedBytes.decodeShort(inputView, ord));
				assertEquals("Did not consume enough bytes.", 2, inputStream.getPosition() - 1);
			}
		}

		/*
		 * assert natural sort order is preserved by the codec.
		 */
		for (Order ord : new Order[]{Order.ASCENDING, Order.DESCENDING}) {
			byte[][] encoded = new byte[vals.length][];
			ByteArrayOutputStreamWithPos outputStream = new ByteArrayOutputStreamWithPos();
			DataOutputViewStreamWrapper outputView = new DataOutputViewStreamWrapper(outputStream);
			for (int i = 0; i < vals.length; i++) {
				outputStream.reset();
				orderedBytes.encodeShort(outputView, vals[i], ord);
				encoded[i] = outputStream.toByteArray();
			}

			Arrays.sort(encoded, new ByteArrayComparator());
			Short[] sortedVals = Arrays.copyOf(vals, vals.length);
			if (ord == Order.ASCENDING) {
				Arrays.sort(sortedVals);
			} else {
				Arrays.sort(sortedVals, Collections.reverseOrder());
			}

			for (int i = 0; i < sortedVals.length; i++) {
				ByteArrayInputStreamWithPos inputStream = new ByteArrayInputStreamWithPos(encoded[i]);
				DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(inputStream);
				int decoded = orderedBytes.decodeShort(inputView, ord);
				assertEquals(
					String.format(
						"Encoded representations do not preserve natural order: <%s>, <%s>, %s",
						sortedVals[i], decoded, ord),
					sortedVals[i].shortValue(), decoded);
			}
		}
	}

	/**
	 * Test int encoding.
	 */
	@Test
	public void testInt() throws IOException {
		OrderedBytes orderedBytes = new OrderedBytes();
		Integer[] vals = {Integer.MIN_VALUE, Integer.MIN_VALUE / 2, 0, Integer.MAX_VALUE / 2, Integer.MAX_VALUE};

		/*
		 * assert encoded values match decoded values.
		 */
		for (Order ord : new Order[]{Order.ASCENDING, Order.DESCENDING}) {
			for (int i = 0; i < vals.length; i++) {
				ByteArrayOutputStreamWithPos outputStream = new ByteArrayOutputStreamWithPos();
				DataOutputViewStreamWrapper outputView = new DataOutputViewStreamWrapper(outputStream);
				outputView.skipBytesToWrite(1);

				// verify encode
				assertEquals("Surprising return value.",
							4, orderedBytes.encodeInt(outputView, vals[i], ord));
				assertEquals("Surprising serialized length.", 4, outputStream.getPosition() - 1);

				// verify decode
				ByteArrayInputStreamWithPos inputStream = new ByteArrayInputStreamWithPos(outputStream.toByteArray());
				DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(inputStream);
				inputView.skipBytesToRead(1);
				assertEquals("Deserialization failed.",
							vals[i].intValue(), orderedBytes.decodeInt(inputView, ord));
				assertEquals("Did not consume enough bytes.", 4, inputStream.getPosition() - 1);
			}
		}

		/*
		 * assert natural sort order is preserved by the codec.
		 */
		for (Order ord : new Order[]{Order.ASCENDING, Order.DESCENDING}) {
			byte[][] encoded = new byte[vals.length][];
			ByteArrayOutputStreamWithPos outputStream = new ByteArrayOutputStreamWithPos();
			DataOutputViewStreamWrapper outputView = new DataOutputViewStreamWrapper(outputStream);
			for (int i = 0; i < vals.length; i++) {
				outputStream.reset();
				orderedBytes.encodeInt(outputView, vals[i], ord);
				encoded[i] = outputStream.toByteArray();
			}

			Arrays.sort(encoded, new ByteArrayComparator());
			Integer[] sortedVals = Arrays.copyOf(vals, vals.length);
			if (ord == Order.ASCENDING) {
				Arrays.sort(sortedVals);
			} else {
				Arrays.sort(sortedVals, Collections.reverseOrder());
			}

			for (int i = 0; i < sortedVals.length; i++) {
				ByteArrayInputStreamWithPos inputStream = new ByteArrayInputStreamWithPos(encoded[i]);
				DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(inputStream);
				int decoded = orderedBytes.decodeInt(inputView, ord);
				assertEquals(
					String.format(
						"Encoded representations do not preserve natural order: <%s>, <%s>, %s",
						sortedVals[i], decoded, ord),
					sortedVals[i].intValue(), decoded);
			}
		}
	}

	/**
	 * Test long encoding.
	 */
	@Test
	public void testLong() throws IOException {
		OrderedBytes orderedBytes = new OrderedBytes();
		Long[] vals = {Long.MIN_VALUE, Long.MIN_VALUE / 2, 0L, Long.MAX_VALUE / 2, Long.MAX_VALUE};

		/*
		 * assert encoded values match decoded values.
		 */
		for (Order ord : new Order[]{Order.ASCENDING, Order.DESCENDING}) {
			for (int i = 0; i < vals.length; i++) {
				ByteArrayOutputStreamWithPos outputStream = new ByteArrayOutputStreamWithPos();
				DataOutputViewStreamWrapper outputView = new DataOutputViewStreamWrapper(outputStream);
				outputView.skipBytesToWrite(1);

				// verify encode
				assertEquals("Surprising return value.",
							8, orderedBytes.encodeLong(outputView, vals[i], ord));
				assertEquals("Surprising serialized length.", 8, outputStream.getPosition() - 1);

				// verify decode
				ByteArrayInputStreamWithPos inputStream = new ByteArrayInputStreamWithPos(outputStream.toByteArray());
				DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(inputStream);
				inputView.skipBytesToRead(1);
				assertEquals("Deserialization failed.",
							vals[i].longValue(), orderedBytes.decodeLong(inputView, ord));
				assertEquals("Did not consume enough bytes.", 8, inputStream.getPosition() - 1);
			}
		}

		/*
		 * assert natural sort order is preserved by the codec.
		 */
		for (Order ord : new Order[]{Order.ASCENDING, Order.DESCENDING}) {
			byte[][] encoded = new byte[vals.length][];
			ByteArrayOutputStreamWithPos outputStream = new ByteArrayOutputStreamWithPos();
			DataOutputViewStreamWrapper outputView = new DataOutputViewStreamWrapper(outputStream);
			for (int i = 0; i < vals.length; i++) {
				outputStream.reset();
				orderedBytes.encodeLong(outputView, vals[i], ord);
				encoded[i] = outputStream.toByteArray();
			}

			Arrays.sort(encoded, new ByteArrayComparator());
			Long[] sortedVals = Arrays.copyOf(vals, vals.length);
			if (ord == Order.ASCENDING) {
				Arrays.sort(sortedVals);
			} else {
				Arrays.sort(sortedVals, Collections.reverseOrder());
			}

			for (int i = 0; i < sortedVals.length; i++) {
				ByteArrayInputStreamWithPos inputStream = new ByteArrayInputStreamWithPos(encoded[i]);
				DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(inputStream);
				long decoded = orderedBytes.decodeLong(inputView, ord);
				assertEquals(
					String.format(
						"Encoded representations do not preserve natural order: <%s>, <%s>, %s",
						sortedVals[i], decoded, ord),
					sortedVals[i].longValue(), decoded);
			}
		}
	}

	/**
	 * Test float encoding.
	 */
	@Test
	public void testFloat() throws IOException {
		OrderedBytes orderedBytes = new OrderedBytes();
		Float[] vals = {Float.MIN_VALUE, Float.MIN_VALUE + 1.0f, 0.0f, Float.MAX_VALUE / 2.0f, Float.MAX_VALUE};

		/*
		 * assert encoded values match decoded values.
		 */
		for (Order ord : new Order[]{Order.ASCENDING, Order.DESCENDING}) {
			for (int i = 0; i < vals.length; i++) {
				ByteArrayOutputStreamWithPos outputStream = new ByteArrayOutputStreamWithPos();
				DataOutputViewStreamWrapper outputView = new DataOutputViewStreamWrapper(outputStream);
				outputView.skipBytesToWrite(1);

				// verify encode
				assertEquals("Surprising return value.",
							4, orderedBytes.encodeFloat(outputView, vals[i], ord));
				assertEquals("Surprising serialized length.", 4, outputStream.getPosition() - 1);

				// verify decode
				ByteArrayInputStreamWithPos inputStream = new ByteArrayInputStreamWithPos(outputStream.toByteArray());
				DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(inputStream);
				inputView.skipBytesToRead(1);
				assertEquals(
					"Deserialization failed.",
					Float.floatToIntBits(vals[i].floatValue()),
					Float.floatToIntBits(orderedBytes.decodeFloat(inputView, ord)));
				assertEquals("Did not consume enough bytes.", 4, inputStream.getPosition() - 1);
			}
		}

		/*
		 * assert natural sort order is preserved by the codec.
		 */
		for (Order ord : new Order[]{Order.ASCENDING, Order.DESCENDING}) {
			byte[][] encoded = new byte[vals.length][];
			ByteArrayOutputStreamWithPos outputStream = new ByteArrayOutputStreamWithPos();
			DataOutputViewStreamWrapper outputView = new DataOutputViewStreamWrapper(outputStream);
			for (int i = 0; i < vals.length; i++) {
				outputStream.reset();
				orderedBytes.encodeFloat(outputView, vals[i], ord);
				encoded[i] = outputStream.toByteArray();
			}

			Arrays.sort(encoded, new ByteArrayComparator());
			Float[] sortedVals = Arrays.copyOf(vals, vals.length);
			if (ord == Order.ASCENDING) {
				Arrays.sort(sortedVals);
			} else {
				Arrays.sort(sortedVals, Collections.reverseOrder());
			}

			for (int i = 0; i < sortedVals.length; i++) {
				ByteArrayInputStreamWithPos inputStream = new ByteArrayInputStreamWithPos(encoded[i]);
				DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(inputStream);
				float decoded = orderedBytes.decodeFloat(inputView, ord);
				assertEquals(
					String.format(
						"Encoded representations do not preserve natural order: <%s>, <%s>, %s",
						sortedVals[i], decoded, ord),
					Float.floatToIntBits(sortedVals[i].floatValue()),
					Float.floatToIntBits(decoded));
			}
		}
	}

	/**
	 * Test double encoding.
	 */
	@Test
	public void testDouble() throws IOException {
		OrderedBytes orderedBytes = new OrderedBytes();
		Double[] vals = {Double.MIN_VALUE, Double.MIN_VALUE + 1.0, 0.0, Double.MAX_VALUE / 2.0, Double.MAX_VALUE};

		/*
		 * assert encoded values match decoded values.
		 */
		for (Order ord : new Order[]{Order.ASCENDING, Order.DESCENDING}) {
			for (int i = 0; i < vals.length; i++) {
				ByteArrayOutputStreamWithPos outputStream = new ByteArrayOutputStreamWithPos();
				DataOutputViewStreamWrapper outputView = new DataOutputViewStreamWrapper(outputStream);
				outputView.skipBytesToWrite(1);

				// verify encode
				assertEquals("Surprising return value.",
							8, orderedBytes.encodeDouble(outputView, vals[i], ord));
				assertEquals("Surprising serialized length.", 8, outputStream.getPosition() - 1);

				// verify decode
				ByteArrayInputStreamWithPos inputStream = new ByteArrayInputStreamWithPos(outputStream.toByteArray());
				DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(inputStream);
				inputView.skipBytesToRead(1);
				assertEquals(
					"Deserialization failed.",
					Double.doubleToLongBits(vals[i].doubleValue()),
					Double.doubleToLongBits(orderedBytes.decodeDouble(inputView, ord)));
				assertEquals("Did not consume enough bytes.", 8, inputStream.getPosition() - 1);
			}
		}

		/*
		 * assert natural sort order is preserved by the codec.
		 */
		for (Order ord : new Order[]{Order.ASCENDING, Order.DESCENDING}) {
			byte[][] encoded = new byte[vals.length][];
			ByteArrayOutputStreamWithPos outputStream = new ByteArrayOutputStreamWithPos();
			DataOutputViewStreamWrapper outputView = new DataOutputViewStreamWrapper(outputStream);
			for (int i = 0; i < vals.length; i++) {
				outputStream.reset();
				orderedBytes.encodeDouble(outputView, vals[i], ord);
				encoded[i] = outputStream.toByteArray();
			}

			Arrays.sort(encoded, new ByteArrayComparator());
			Double[] sortedVals = Arrays.copyOf(vals, vals.length);
			if (ord == Order.ASCENDING) {
				Arrays.sort(sortedVals);
			} else {
				Arrays.sort(sortedVals, Collections.reverseOrder());
			}

			for (int i = 0; i < sortedVals.length; i++) {
				ByteArrayInputStreamWithPos inputStream = new ByteArrayInputStreamWithPos(encoded[i]);
				DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(inputStream);
				double decoded = orderedBytes.decodeDouble(inputView, ord);
				assertEquals(
					String.format(
						"Encoded representations do not preserve natural order: <%s>, <%s>, %s",
						sortedVals[i], decoded, ord),
					Double.doubleToLongBits(sortedVals[i].doubleValue()),
					Double.doubleToLongBits(decoded));
			}
		}
	}

	/**
	 * Test string encoding.
	 */
	@Test
	public void testString() throws IOException {
		OrderedBytes orderedBytes = new OrderedBytes();
		String[] vals = {"", "foo", "fooa", "baaaar", "bazz"};
		int[] expectedLengths = {0, 3, 4, 6, 4};

		/*
		 * assert encoded values match decoded values.
		 */
		for (Order ord : new Order[] { Order.ASCENDING, Order.DESCENDING }) {
			for (int i = 0; i < vals.length; i++) {
				ByteArrayOutputStreamWithPos outputStream = new ByteArrayOutputStreamWithPos();
				DataOutputViewStreamWrapper outputView = new DataOutputViewStreamWrapper(outputStream);
				outputView.skipBytesToWrite(1);

				int encodeLength = expectedLengths[i] + (ord == Order.DESCENDING ? 1 : 0);
				// verify encode
				assertEquals("Surprising return value.",
							encodeLength, orderedBytes.encodeString(outputView, vals[i], ord));
				assertEquals("Surprising serialized length.", encodeLength, outputStream.getPosition() - 1);

				// verify decode
				ByteArrayInputStreamWithPos inputStream = new ByteArrayInputStreamWithPos(outputStream.toByteArray());
				DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(inputStream);
				inputView.skipBytesToRead(1);
				assertEquals("Deserialization failed.", vals[i], orderedBytes.decodeString(inputView, ord));
				assertEquals("Did not consume enough bytes.", encodeLength, inputStream.getPosition() - 1);
			}
		}

		/*
		 * assert natural sort order is preserved by the codec.
		 */
		for (Order ord : new Order[]{Order.ASCENDING, Order.DESCENDING}) {
			byte[][] encoded = new byte[vals.length][];
			ByteArrayOutputStreamWithPos outputStream = new ByteArrayOutputStreamWithPos();
			DataOutputViewStreamWrapper outputView = new DataOutputViewStreamWrapper(outputStream);
			for (int i = 0; i < vals.length; i++) {
				outputStream.reset();
				orderedBytes.encodeString(outputView, vals[i], ord);
				encoded[i] = outputStream.toByteArray();
			}

			Arrays.sort(encoded, new ByteArrayComparator());
			String[] sortedVals = Arrays.copyOf(vals, vals.length);
			if (ord == Order.ASCENDING) {
				Arrays.sort(sortedVals);
			} else {
				Arrays.sort(sortedVals, Collections.reverseOrder());
			}

			for (int i = 0; i < sortedVals.length; i++) {
				ByteArrayInputStreamWithPos inputStream = new ByteArrayInputStreamWithPos(encoded[i]);
				DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(inputStream);
				String decoded = orderedBytes.decodeString(inputView, ord);
				assertEquals(
					String.format(
						"Encoded representations do not preserve natural order: <%s>, <%s>, %s",
						sortedVals[i], decoded, ord),
					sortedVals[i], decoded);
			}
		}
	}

	/**
	 * Test BinaryString encoding.
	 */
	@Test
	public void testBinaryString() throws IOException {
		OrderedBytes orderedBytes = new OrderedBytes();
		BinaryString[] vals = {
			BinaryString.fromString(""), BinaryString.fromString("foo"), BinaryString.fromString("fooa"),
			BinaryString.fromString("baaaar"), BinaryString.fromString("bazz")
		};
		int[] expectedLengths = {0, 3, 4, 6, 4};

		/*
		 * assert encoded values match decoded values.
		 */
		for (Order ord : new Order[] { Order.ASCENDING, Order.DESCENDING }) {
			for (int i = 0; i < vals.length; i++) {
				ByteArrayOutputStreamWithPos outputStream = new ByteArrayOutputStreamWithPos();
				DataOutputViewStreamWrapper outputView = new DataOutputViewStreamWrapper(outputStream);
				outputView.skipBytesToWrite(1);

				int encodeLength = expectedLengths[i] + (ord == Order.DESCENDING ? 1 : 0);
				// verify encode
				assertEquals("Surprising return value.",
							encodeLength, orderedBytes.encodeBinaryString(outputView, vals[i], ord));
				assertEquals("Surprising serialized length.", encodeLength, outputStream.getPosition() - 1);

				// verify decode
				ByteArrayInputStreamWithPos inputStream = new ByteArrayInputStreamWithPos(outputStream.toByteArray());
				DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(inputStream);
				inputView.skipBytesToRead(1);
				assertEquals("Deserialization failed.", vals[i], orderedBytes.decodeBinaryString(inputView, ord));
				assertEquals("Did not consume enough bytes.", encodeLength, inputStream.getPosition() - 1);
			}
		}

		/*
		 * assert natural sort order is preserved by the codec.
		 */
		for (Order ord : new Order[]{Order.ASCENDING, Order.DESCENDING}) {
			byte[][] encoded = new byte[vals.length][];
			ByteArrayOutputStreamWithPos outputStream = new ByteArrayOutputStreamWithPos();
			DataOutputViewStreamWrapper outputView = new DataOutputViewStreamWrapper(outputStream);
			for (int i = 0; i < vals.length; i++) {
				outputStream.reset();
				orderedBytes.encodeBinaryString(outputView, vals[i], ord);
				encoded[i] = outputStream.toByteArray();
			}

			Arrays.sort(encoded, new ByteArrayComparator());
			BinaryString[] sortedVals = Arrays.copyOf(vals, vals.length);
			if (ord == Order.ASCENDING) {
				Arrays.sort(sortedVals);
			} else {
				Arrays.sort(sortedVals, Collections.reverseOrder());
			}

			for (int i = 0; i < sortedVals.length; i++) {
				ByteArrayInputStreamWithPos inputStream = new ByteArrayInputStreamWithPos(encoded[i]);
				DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(inputStream);
				BinaryString decoded = orderedBytes.decodeBinaryString(inputView, ord);
				assertEquals(
					String.format(
						"Encoded representations do not preserve natural order: <%s>, <%s>, %s",
						sortedVals[i], decoded, ord),
					sortedVals[i], decoded);
			}
		}
	}

	/**
	 * Test byte array encoding.
	 */
	@Test
	public void testByteArray() throws IOException {
		OrderedBytes orderedBytes = new OrderedBytes();
		byte[][] vals =
			{
				"".getBytes(StandardCharsets.UTF_8.name()),
				"foo".getBytes(StandardCharsets.UTF_8.name()),
				"foobarbazbub".getBytes(StandardCharsets.UTF_8.name()),
				{
					(byte) 0xaa, (byte) 0xaa, (byte) 0xaa, (byte) 0xaa, (byte) 0xaa, (byte) 0xaa,
					(byte) 0xaa, (byte) 0xaa, (byte) 0xaa, (byte) 0xaa, (byte) 0xaa, (byte) 0xaa
				},
				{
					(byte) 0x55, (byte) 0x55, (byte) 0x55, (byte) 0x55, (byte) 0x55, (byte) 0x55,
					(byte) 0x55, (byte) 0x55, (byte) 0x55, (byte) 0x55, (byte) 0x55, (byte) 0x55
				},
			};

		/*
		 * assert encoded values match decoded values.
		 */
		for (Order ord : new Order[]{Order.ASCENDING, Order.DESCENDING}) {
			for (byte[] val : vals) {
				ByteArrayOutputStreamWithPos outputStream = new ByteArrayOutputStreamWithPos();
				DataOutputViewStreamWrapper outputView = new DataOutputViewStreamWrapper(outputStream);
				outputView.skipBytesToWrite(1);

				int encodeLength = val.length + (ord == Order.DESCENDING ? 1 : 0);
				// verify encode
				assertEquals("Surprising return value.",
							encodeLength , orderedBytes.encodeByteArray(outputView, val, ord));
				assertEquals("Surprising serialized length.", encodeLength, outputStream.getPosition() - 1);

				// verify decode
				ByteArrayInputStreamWithPos inputStream = new ByteArrayInputStreamWithPos(outputStream.toByteArray());
				DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(inputStream);
				inputView.skipBytesToRead(1);
				assertArrayEquals("Deserialization failed.", val, orderedBytes.decodeByteArray(inputView, ord));
				assertEquals("Did not consume enough bytes.", encodeLength, inputStream.getPosition() - 1);
			}
		}

		/*
		 * assert natural sort order is preserved by the codec.
		 */
		for (Order ord : new Order[]{Order.DESCENDING}) {
			byte[][] encoded = new byte[vals.length][];
			ByteArrayOutputStreamWithPos outputStream = new ByteArrayOutputStreamWithPos();
			DataOutputViewStreamWrapper outputView = new DataOutputViewStreamWrapper(outputStream);
			for (int i = 0; i < vals.length; i++) {
				outputStream.reset();
				orderedBytes.encodeByteArray(outputView, vals[i], ord);
				encoded[i] = outputStream.toByteArray();
			}

			Arrays.sort(encoded, new ByteArrayComparator());
			byte[][] sortedVals = Arrays.copyOf(vals, vals.length);
			Arrays.sort(sortedVals, new ByteArrayComparator(ord == Order.ASCENDING));

			for (int i = 0; i < sortedVals.length; i++) {
				ByteArrayInputStreamWithPos inputStream = new ByteArrayInputStreamWithPos(encoded[i]);
				DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(inputStream);
				byte[] decoded = orderedBytes.decodeByteArray(inputView, ord);
				assertArrayEquals(
					String.format(
						"Encoded representations do not preserve natural order: <%s>, <%s>, %s",
						Arrays.toString(sortedVals[i]), Arrays.toString(decoded), ord),
					sortedVals[i], decoded);
			}
		}
	}
}
