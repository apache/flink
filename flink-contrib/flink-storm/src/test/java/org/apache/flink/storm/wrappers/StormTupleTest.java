/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.storm.wrappers;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.storm.util.AbstractTest;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.MessageId;
import org.apache.storm.tuple.Values;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;

/**
 * Tests for the StormTuple.
 */
public class StormTupleTest extends AbstractTest {
	private static final String fieldName = "fieldName";
	private static final String fieldNamePojo = "member";

	private int arity, index;

	@Override
	@Before
	public void prepare() {
		super.prepare();
		this.arity = 1 + r.nextInt(25);
		this.index = r.nextInt(this.arity);
	}

	@Test
	public void nonTupleTest() {
		final Object flinkTuple = this.r.nextInt();

		final StormTuple<Object> tuple = new StormTuple<Object>(flinkTuple, null, -1, null, null,
				null);
		Assert.assertSame(flinkTuple, tuple.getValue(0));

		final List<Object> values = tuple.getValues();
		Assert.assertEquals(1, values.size());
		Assert.assertEquals(flinkTuple, values.get(0));
	}

	@Test
	public void tupleTest() throws InstantiationException, IllegalAccessException {
		for (int numberOfAttributes = 0; numberOfAttributes < 26; ++numberOfAttributes) {
			final Object[] data = new Object[numberOfAttributes];

			final Tuple flinkTuple = Tuple.getTupleClass(numberOfAttributes).newInstance();
			for (int i = 0; i < numberOfAttributes; ++i) {
				data[i] = this.r.nextInt();
				flinkTuple.setField(data[i], i);
			}

			final StormTuple<Tuple> tuple = new StormTuple<Tuple>(flinkTuple, null, -1, null, null,
					null);
			final List<Object> values = tuple.getValues();

			Assert.assertEquals(numberOfAttributes, values.size());
			for (int i = 0; i < numberOfAttributes; ++i) {
				Assert.assertEquals(flinkTuple.getField(i), values.get(i));
			}

			Assert.assertEquals(numberOfAttributes, tuple.size());
		}
	}

	@Test
	public void tupleTestWithTaskId() throws InstantiationException, IllegalAccessException {
		for (int numberOfAttributes = 1; numberOfAttributes < 26; ++numberOfAttributes) {
			final Object[] data = new Object[numberOfAttributes];

			final Tuple flinkTuple = Tuple.getTupleClass(numberOfAttributes).newInstance();
			for (int i = 0; i < numberOfAttributes - 1; ++i) {
				data[i] = this.r.nextInt();
				flinkTuple.setField(data[i], i);
			}

			final StormTuple<Tuple> tuple = new StormTuple<Tuple>(flinkTuple, null, 0, null, null,
					null);
			final List<Object> values = tuple.getValues();

			Assert.assertEquals(numberOfAttributes - 1, values.size());
			for (int i = 0; i < numberOfAttributes - 1; ++i) {
				Assert.assertEquals(flinkTuple.getField(i), values.get(i));
			}

			Assert.assertEquals(numberOfAttributes - 1, tuple.size());
		}
	}

	@Test
	public void testBinary() {
		final byte[] data = new byte[this.r.nextInt(15)];
		this.r.nextBytes(data);

		final int index = this.r.nextInt(5);
		final Tuple flinkTuple = new Tuple5<Object, Object, Object, Object, Object>();
		flinkTuple.setField(data, index);

		final StormTuple<Tuple> tuple = new StormTuple<Tuple>(flinkTuple, null, -1, null, null,
				null);
		Assert.assertEquals(flinkTuple.getField(index), tuple.getBinary(index));
	}

	@Test
	public void testBoolean() {
		final Boolean flinkTuple = this.r.nextBoolean();

		final StormTuple<Boolean> tuple = new StormTuple<Boolean>(flinkTuple, null, -1, null, null,
				null);
		Assert.assertEquals(flinkTuple, tuple.getBoolean(0));
	}

	@Test
	public void testByte() {
		final Byte flinkTuple = (byte) this.r.nextInt();

		final StormTuple<Byte> tuple = new StormTuple<Byte>(flinkTuple, null, -1, null, null, null);
		Assert.assertEquals(flinkTuple, tuple.getByte(0));
	}

	@Test
	public void testDouble() {
		final Double flinkTuple = this.r.nextDouble();

		final StormTuple<Double> tuple = new StormTuple<Double>(flinkTuple, null, -1, null, null,
				null);
		Assert.assertEquals(flinkTuple, tuple.getDouble(0));
	}

	@Test
	public void testFloat() {
		final Float flinkTuple = this.r.nextFloat();

		final StormTuple<Float> tuple = new StormTuple<Float>(flinkTuple, null, -1, null, null,
				null);
		Assert.assertEquals(flinkTuple, tuple.getFloat(0));
	}

	@Test
	public void testInteger() {
		final Integer flinkTuple = this.r.nextInt();

		final StormTuple<Integer> tuple = new StormTuple<Integer>(flinkTuple, null, -1, null, null,
				null);
		Assert.assertEquals(flinkTuple, tuple.getInteger(0));
	}

	@Test
	public void testLong() {
		final Long flinkTuple = this.r.nextLong();

		final StormTuple<Long> tuple = new StormTuple<Long>(flinkTuple, null, -1, null, null, null);
		Assert.assertEquals(flinkTuple, tuple.getLong(0));
	}

	@Test
	public void testShort() {
		final Short flinkTuple = (short) this.r.nextInt();

		final StormTuple<Short> tuple = new StormTuple<Short>(flinkTuple, null, -1, null, null,
				null);
		Assert.assertEquals(flinkTuple, tuple.getShort(0));
	}

	@Test
	public void testString() {
		final byte[] data = new byte[this.r.nextInt(15)];
		this.r.nextBytes(data);
		final String flinkTuple = new String(data, ConfigConstants.DEFAULT_CHARSET);

		final StormTuple<String> tuple = new StormTuple<String>(flinkTuple, null, -1, null, null,
				null);
		Assert.assertEquals(flinkTuple, tuple.getString(0));
	}

	@Test
	public void testBinaryTuple() {
		final byte[] data = new byte[this.r.nextInt(15)];
		this.r.nextBytes(data);

		final int index = this.r.nextInt(5);
		final Tuple flinkTuple = new Tuple5<Object, Object, Object, Object, Object>();
		flinkTuple.setField(data, index);

		final StormTuple<Tuple> tuple = new StormTuple<Tuple>(flinkTuple, null, -1, null, null,
				null);
		Assert.assertEquals(flinkTuple.getField(index), tuple.getBinary(index));
	}

	@Test
	public void testBooleanTuple() {
		final Boolean data = this.r.nextBoolean();

		final int index = this.r.nextInt(5);
		final Tuple flinkTuple = new Tuple5<Object, Object, Object, Object, Object>();
		flinkTuple.setField(data, index);

		final StormTuple<Tuple> tuple = new StormTuple<Tuple>(flinkTuple, null, -1, null, null,
				null);
		Assert.assertEquals(flinkTuple.getField(index), tuple.getBoolean(index));
	}

	@Test
	public void testByteTuple() {
		final Byte data = (byte) this.r.nextInt();

		final int index = this.r.nextInt(5);
		final Tuple flinkTuple = new Tuple5<Object, Object, Object, Object, Object>();
		flinkTuple.setField(data, index);

		final StormTuple<Tuple> tuple = new StormTuple<Tuple>(flinkTuple, null, -1, null, null,
				null);
		Assert.assertEquals(flinkTuple.getField(index), tuple.getByte(index));
	}

	@Test
	public void testDoubleTuple() {
		final Double data = this.r.nextDouble();

		final int index = this.r.nextInt(5);
		final Tuple flinkTuple = new Tuple5<Object, Object, Object, Object, Object>();
		flinkTuple.setField(data, index);

		final StormTuple<Tuple> tuple = new StormTuple<Tuple>(flinkTuple, null, -1, null, null,
				null);
		Assert.assertEquals(flinkTuple.getField(index), tuple.getDouble(index));
	}

	@Test
	public void testFloatTuple() {
		final Float data = this.r.nextFloat();

		final int index = this.r.nextInt(5);
		final Tuple flinkTuple = new Tuple5<Object, Object, Object, Object, Object>();
		flinkTuple.setField(data, index);

		final StormTuple<Tuple> tuple = new StormTuple<Tuple>(flinkTuple, null, -1, null, null,
				null);
		Assert.assertEquals(flinkTuple.getField(index), tuple.getFloat(index));
	}

	@Test
	public void testIntegerTuple() {
		final Integer data = this.r.nextInt();

		final int index = this.r.nextInt(5);
		final Tuple flinkTuple = new Tuple5<Object, Object, Object, Object, Object>();
		flinkTuple.setField(data, index);

		final StormTuple<Tuple> tuple = new StormTuple<Tuple>(flinkTuple, null, -1, null, null,
				null);
		Assert.assertEquals(flinkTuple.getField(index), tuple.getInteger(index));
	}

	@Test
	public void testLongTuple() {
		final Long data = this.r.nextLong();

		final int index = this.r.nextInt(5);
		final Tuple flinkTuple = new Tuple5<Object, Object, Object, Object, Object>();
		flinkTuple.setField(data, index);

		final StormTuple<Tuple> tuple = new StormTuple<Tuple>(flinkTuple, null, -1, null, null,
				null);
		Assert.assertEquals(flinkTuple.getField(index), tuple.getLong(index));
	}

	@Test
	public void testShortTuple() {
		final Short data = (short) this.r.nextInt();

		final int index = this.r.nextInt(5);
		final Tuple flinkTuple = new Tuple5<Object, Object, Object, Object, Object>();
		flinkTuple.setField(data, index);

		final StormTuple<Tuple> tuple = new StormTuple<Tuple>(flinkTuple, null, -1, null, null,
				null);
		Assert.assertEquals(flinkTuple.getField(index), tuple.getShort(index));
	}

	@Test
	public void testStringTuple() {
		final byte[] rawdata = new byte[this.r.nextInt(15)];
		this.r.nextBytes(rawdata);
		final String data = new String(rawdata, ConfigConstants.DEFAULT_CHARSET);

		final int index = this.r.nextInt(5);
		final Tuple flinkTuple = new Tuple5<Object, Object, Object, Object, Object>();
		flinkTuple.setField(data, index);

		final StormTuple<Tuple> tuple = new StormTuple<Tuple>(flinkTuple, null, -1, null, null,
				null);
		Assert.assertEquals(flinkTuple.getField(index), tuple.getString(index));
	}

	@Test
	public void testContains() throws Exception {
		Fields schema = new Fields("a1", "a2");
		StormTuple<Object> tuple = new StormTuple<Object>(Tuple.getTupleClass(1).newInstance(),
				schema, -1, null, null, null);

		Assert.assertTrue(tuple.contains("a1"));
		Assert.assertTrue(tuple.contains("a2"));
		Assert.assertFalse(tuple.contains("a3"));
	}

	@Test
	public void testGetFields() throws Exception {
		Fields schema = new Fields();
		Assert.assertSame(schema,
				new StormTuple<Object>(null, schema, -1, null, null, null).getFields());

	}

	@Test
	public void testFieldIndex() throws Exception {
		Fields schema = new Fields("a1", "a2");
		StormTuple<Object> tuple = new StormTuple<Object>(Tuple.getTupleClass(1).newInstance(),
				schema, -1, null, null, null);

		Assert.assertEquals(0, tuple.fieldIndex("a1"));
		Assert.assertEquals(1, tuple.fieldIndex("a2"));
	}

	@Test
	public void testSelect() throws Exception {
		Tuple tuple = Tuple.getTupleClass(arity).newInstance();
		Values values = new Values();

		ArrayList<String> attributeNames = new ArrayList<String>(arity);
		ArrayList<String> filterNames = new ArrayList<String>(arity);

		for (int i = 0; i < arity; ++i) {
			tuple.setField(i, i);
			attributeNames.add("a" + i);

			if (r.nextBoolean()) {
				filterNames.add("a" + i);
				values.add(i);
			}
		}
		Fields schema = new Fields(attributeNames);
		Fields selector = new Fields(filterNames);

		Assert.assertEquals(values,
				new StormTuple<>(tuple, schema, -1, null, null, null).select(selector));
	}

	@Test
	public void testGetValueByField() throws Exception {
		Object value = mock(Object.class);
		StormTuple<?> tuple = testGetByField(arity, index, value);
		Assert.assertSame(value, tuple.getValueByField(fieldName));

	}

	@Test
	public void testGetValueByFieldPojo() throws Exception {
		Object value = mock(Object.class);
		TestPojoMember<Object> pojo = new TestPojoMember<Object>(value);
		StormTuple<TestPojoMember<Object>> tuple = new StormTuple<TestPojoMember<Object>>(pojo,
				null, -1, null, null, null);
		Assert.assertSame(value, tuple.getValueByField(fieldNamePojo));
	}

	@Test
	public void testGetValueByFieldPojoGetter() throws Exception {
		Object value = mock(Object.class);
		TestPojoGetter<Object> pojo = new TestPojoGetter<Object>(value);
		StormTuple<TestPojoGetter<Object>> tuple = new StormTuple<TestPojoGetter<Object>>(pojo,
				null, -1, null, null, null);
		Assert.assertSame(value, tuple.getValueByField(fieldNamePojo));
	}

	@Test
	public void testGetStringByField() throws Exception {
		String value = "stringValue";
		StormTuple<?> tuple = testGetByField(arity, index, value);
		Assert.assertSame(value, tuple.getStringByField(fieldName));
	}

	@Test
	public void testGetStringByFieldPojo() throws Exception {
		String value = "stringValue";
		TestPojoMember<String> pojo = new TestPojoMember<String>(value);
		StormTuple<TestPojoMember<String>> tuple = new StormTuple<TestPojoMember<String>>(pojo,
				null, -1, null, null, null);
		Assert.assertSame(value, tuple.getStringByField(fieldNamePojo));
	}

	@Test
	public void testGetStringByFieldPojoGetter() throws Exception {
		String value = "stringValue";
		TestPojoGetter<String> pojo = new TestPojoGetter<String>(value);
		StormTuple<TestPojoGetter<String>> tuple = new StormTuple<TestPojoGetter<String>>(pojo,
				null, -1, null, null, null);
		Assert.assertSame(value, tuple.getStringByField(fieldNamePojo));
	}

	@Test
	public void testGetIntegerByField() throws Exception {
		Integer value = r.nextInt();
		StormTuple<?> tuple = testGetByField(arity, index, value);
		Assert.assertSame(value, tuple.getIntegerByField(fieldName));
	}

	@Test
	public void testGetIntegerByFieldPojo() throws Exception {
		Integer value = r.nextInt();
		TestPojoMember<Integer> pojo = new TestPojoMember<Integer>(value);
		StormTuple<TestPojoMember<Integer>> tuple = new StormTuple<TestPojoMember<Integer>>(pojo,
				null, -1, null, null, null);
		Assert.assertSame(value, tuple.getIntegerByField(fieldNamePojo));
	}

	@Test
	public void testGetIntegerByFieldPojoGetter() throws Exception {
		Integer value = r.nextInt();
		TestPojoGetter<Integer> pojo = new TestPojoGetter<Integer>(value);
		StormTuple<TestPojoGetter<Integer>> tuple = new StormTuple<TestPojoGetter<Integer>>(pojo,
				null, -1, null, null, null);
		Assert.assertSame(value, tuple.getIntegerByField(fieldNamePojo));
	}

	@Test
	public void testGetLongByField() throws Exception {
		Long value = r.nextLong();
		StormTuple<?> tuple = testGetByField(arity, index, value);
		Assert.assertSame(value, tuple.getLongByField(fieldName));
	}

	@Test
	public void testGetLongByFieldPojo() throws Exception {
		Long value = r.nextLong();
		TestPojoMember<Long> pojo = new TestPojoMember<Long>(value);
		StormTuple<TestPojoMember<Long>> tuple = new StormTuple<TestPojoMember<Long>>(pojo, null,
				-1, null, null, null);
		Assert.assertSame(value, tuple.getLongByField(fieldNamePojo));
	}

	@Test
	public void testGetLongByFieldPojoGetter() throws Exception {
		Long value = r.nextLong();
		TestPojoGetter<Long> pojo = new TestPojoGetter<Long>(value);
		StormTuple<TestPojoGetter<Long>> tuple = new StormTuple<TestPojoGetter<Long>>(pojo, null,
				-1, null, null, null);
		Assert.assertSame(value, tuple.getLongByField(fieldNamePojo));
	}

	@Test
	public void testGetBooleanByField() throws Exception {
		Boolean value = r.nextBoolean();
		StormTuple<?> tuple = testGetByField(arity, index, value);
		Assert.assertEquals(value, tuple.getBooleanByField(fieldName));
	}

	@Test
	public void testGetBooleanByFieldPojo() throws Exception {
		Boolean value = r.nextBoolean();
		TestPojoMember<Boolean> pojo = new TestPojoMember<Boolean>(value);
		StormTuple<TestPojoMember<Boolean>> tuple = new StormTuple<TestPojoMember<Boolean>>(pojo,
				null, -1, null, null, null);
		Assert.assertSame(value, tuple.getBooleanByField(fieldNamePojo));
	}

	@Test
	public void testGetBooleanByFieldPojoGetter() throws Exception {
		Boolean value = r.nextBoolean();
		TestPojoGetter<Boolean> pojo = new TestPojoGetter<Boolean>(value);
		StormTuple<TestPojoGetter<Boolean>> tuple = new StormTuple<TestPojoGetter<Boolean>>(pojo,
				null, -1, null, null, null);
		Assert.assertSame(value, tuple.getBooleanByField(fieldNamePojo));
	}

	@Test
	public void testGetShortByField() throws Exception {
		Short value = (short) r.nextInt();
		StormTuple<?> tuple = testGetByField(arity, index, value);
		Assert.assertSame(value, tuple.getShortByField(fieldName));
	}

	@Test
	public void testGetShortByFieldPojo() throws Exception {
		Short value = (short) r.nextInt();
		TestPojoMember<Short> pojo = new TestPojoMember<Short>(value);
		StormTuple<TestPojoMember<Short>> tuple = new StormTuple<TestPojoMember<Short>>(pojo,
				null,
				-1, null, null, null);
		Assert.assertSame(value, tuple.getShortByField(fieldNamePojo));
	}

	@Test
	public void testGetShortByFieldPojoGetter() throws Exception {
		Short value = (short) r.nextInt();
		TestPojoGetter<Short> pojo = new TestPojoGetter<Short>(value);
		StormTuple<TestPojoGetter<Short>> tuple = new StormTuple<TestPojoGetter<Short>>(pojo,
				null,
				-1, null, null, null);
		Assert.assertSame(value, tuple.getShortByField(fieldNamePojo));
	}

	@Test
	public void testGetByteByField() throws Exception {
		Byte value = new Byte((byte) r.nextInt());
		StormTuple<?> tuple = testGetByField(arity, index, value);
		Assert.assertSame(value, tuple.getByteByField(fieldName));
	}

	@Test
	public void testGetByteByFieldPojo() throws Exception {
		Byte value = new Byte((byte) r.nextInt());
		TestPojoMember<Byte> pojo = new TestPojoMember<Byte>(value);
		StormTuple<TestPojoMember<Byte>> tuple = new StormTuple<TestPojoMember<Byte>>(pojo,
				null,
				-1, null, null, null);
		Assert.assertSame(value, tuple.getByteByField(fieldNamePojo));
	}

	@Test
	public void testGetByteByFieldPojoGetter() throws Exception {
		Byte value = new Byte((byte) r.nextInt());
		TestPojoGetter<Byte> pojo = new TestPojoGetter<Byte>(value);
		StormTuple<TestPojoGetter<Byte>> tuple = new StormTuple<TestPojoGetter<Byte>>(pojo,
				null,
				-1, null, null, null);
		Assert.assertSame(value, tuple.getByteByField(fieldNamePojo));
	}

	@Test
	public void testGetDoubleByField() throws Exception {
		Double value = r.nextDouble();
		StormTuple<?> tuple = testGetByField(arity, index, value);
		Assert.assertSame(value, tuple.getDoubleByField(fieldName));
	}

	@Test
	public void testGetDoubleByFieldPojo() throws Exception {
		Double value = r.nextDouble();
		TestPojoMember<Double> pojo = new TestPojoMember<Double>(value);
		StormTuple<TestPojoMember<Double>> tuple = new StormTuple<TestPojoMember<Double>>(pojo,
				null, -1, null, null, null);
		Assert.assertSame(value, tuple.getDoubleByField(fieldNamePojo));
	}

	@Test
	public void testGetDoubleByFieldPojoGetter() throws Exception {
		Double value = r.nextDouble();
		TestPojoGetter<Double> pojo = new TestPojoGetter<Double>(value);
		StormTuple<TestPojoGetter<Double>> tuple = new StormTuple<TestPojoGetter<Double>>(pojo,
				null, -1, null, null, null);
		Assert.assertSame(value, tuple.getDoubleByField(fieldNamePojo));
	}

	@Test
	public void testGetFloatByField() throws Exception {
		Float value = r.nextFloat();
		StormTuple<?> tuple = testGetByField(arity, index, value);
		Assert.assertSame(value, tuple.getFloatByField(fieldName));
	}

	@Test
	public void testGetFloatByFieldPojo() throws Exception {
		Float value = r.nextFloat();
		TestPojoMember<Float> pojo = new TestPojoMember<Float>(value);
		StormTuple<TestPojoMember<Float>> tuple = new StormTuple<TestPojoMember<Float>>(pojo,
				null,
				-1, null, null, null);
		Assert.assertSame(value, tuple.getFloatByField(fieldNamePojo));
	}

	@Test
	public void testGetFloatByFieldPojoGetter() throws Exception {
		Float value = r.nextFloat();
		TestPojoGetter<Float> pojo = new TestPojoGetter<Float>(value);
		StormTuple<TestPojoGetter<Float>> tuple = new StormTuple<TestPojoGetter<Float>>(pojo,
				null,
				-1, null, null, null);
		Assert.assertSame(value, tuple.getFloatByField(fieldNamePojo));
	}

	@Test
	public void testGetBinaryByField() throws Exception {
		byte[] data = new byte[1 + r.nextInt(20)];
		r.nextBytes(data);
		StormTuple<?> tuple = testGetByField(arity, index, data);
		Assert.assertSame(data, tuple.getBinaryByField(fieldName));
	}

	@Test
	public void testGetBinaryFieldPojo() throws Exception {
		byte[] data = new byte[1 + r.nextInt(20)];
		r.nextBytes(data);
		TestPojoMember<byte[]> pojo = new TestPojoMember<byte[]>(data);
		StormTuple<TestPojoMember<byte[]>> tuple = new StormTuple<TestPojoMember<byte[]>>(pojo,
				null, -1, null, null, null);
		Assert.assertSame(data, tuple.getBinaryByField(fieldNamePojo));
	}

	@Test
	public void testGetBinaryByFieldPojoGetter() throws Exception {
		byte[] data = new byte[1 + r.nextInt(20)];
		r.nextBytes(data);
		TestPojoGetter<byte[]> pojo = new TestPojoGetter<byte[]>(data);
		StormTuple<TestPojoGetter<byte[]>> tuple = new StormTuple<TestPojoGetter<byte[]>>(pojo,
				null, -1, null, null, null);
		Assert.assertSame(data, tuple.getBinaryByField(fieldNamePojo));
	}

	private <T> StormTuple<?> testGetByField(int arity, int index, T value)
			throws Exception {

		assert (index < arity);

		Tuple tuple = Tuple.getTupleClass(arity).newInstance();
		tuple.setField(value, index);

		ArrayList<String> attributeNames = new ArrayList<String>(arity);
		for (int i = 0; i < arity; ++i) {
			if (i == index) {
				attributeNames.add(fieldName);
			} else {
				attributeNames.add("" + i);
			}
		}
		Fields schema = new Fields(attributeNames);

		return new StormTuple<>(tuple, schema, -1, null, null, null);
	}

	@Test
	public void testGetSourceGlobalStreamid() {
		GlobalStreamId globalStreamid = new StormTuple<>(null, null, -1, "streamId", "componentID",
				null).getSourceGlobalStreamid();
		Assert.assertEquals("streamId", globalStreamid.get_streamId());
		Assert.assertEquals("componentID", globalStreamid.get_componentId());
	}

	@Test
	public void testGetSourceComponent() {
		String sourceComponent = new StormTuple<>(null, null, -1, null, "componentID", null)
				.getSourceComponent();
		Assert.assertEquals("componentID", sourceComponent);
	}

	@Test
	public void testGetSourceTask() {
		int sourceTaskId = new StormTuple<>(null, null, 42, null, null, null).getSourceTask();
		Assert.assertEquals(42, sourceTaskId);
	}

	@Test
	public void testGetSourceStreamId() {
		String sourceStreamId = new StormTuple<>(null, null, -1, "streamId", null, null)
				.getSourceStreamId();
		Assert.assertEquals("streamId", sourceStreamId);
	}

	@Test
	public void testGetMessageId() {
		MessageId messageId = MessageId.makeUnanchored();
		StormTuple<?> stormTuple = new StormTuple<>(null, null, -1, null, null, messageId);
		Assert.assertSame(messageId, stormTuple.getMessageId());
	}

	private static class TestPojoMember<T> {
		public T member;

		public TestPojoMember(T value) {
			this.member = value;
		}
	}

	private static class TestPojoGetter<T> {
		private T member;

		public TestPojoGetter(T value) {
			this.member = value;
		}

		public T getMember() {
			return this.member;
		}
	}
}
