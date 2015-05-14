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

package org.apache.flink.stormcompatibility.wrappers;

import java.util.List;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.stormcompatibility.util.AbstractTest;
import org.junit.Assert;
import org.junit.Test;





public class StormTupleTest extends AbstractTest {
	
	@Test
	public void nonTupleTest() {
		final Object flinkTuple = new Integer(this.r.nextInt());
		
		final StormTuple<Object> tuple = new StormTuple<Object>(flinkTuple);
		Assert.assertSame(flinkTuple, tuple.getValue(0));
		
		final List<Object> values = tuple.getValues();
		Assert.assertEquals(1, values.size());
		Assert.assertEquals(flinkTuple, values.get(0));
	}
	
	@Test
	public void tupleTest() throws InstantiationException, IllegalAccessException {
		final int numberOfAttributes = 1 + this.r.nextInt(25);
		final Object[] data = new Object[numberOfAttributes];
		
		final Tuple flinkTuple = Tuple.getTupleClass(numberOfAttributes).newInstance();
		for(int i = 0; i < numberOfAttributes; ++i) {
			data[i] = new Integer(this.r.nextInt());
			flinkTuple.setField(data[i], i);
		}
		
		final StormTuple<Tuple> tuple = new StormTuple<Tuple>(flinkTuple);
		final List<Object> values = tuple.getValues();
		
		Assert.assertEquals(numberOfAttributes, values.size());
		for(int i = 0; i < numberOfAttributes; ++i) {
			Assert.assertEquals(flinkTuple.getField(i), values.get(i));
		}
		
		Assert.assertEquals(numberOfAttributes, tuple.size());
	}
	
	@Test
	public void testBinary() {
		final byte[] data = new byte[this.r.nextInt(15)];
		this.r.nextBytes(data);
		
		final int index = this.r.nextInt(5);
		final Tuple flinkTuple = new Tuple5<Object, Object, Object, Object, Object>();
		flinkTuple.setField(data, index);
		
		final StormTuple<Tuple> tuple = new StormTuple<Tuple>(flinkTuple);
		Assert.assertEquals(flinkTuple.getField(index), tuple.getBinary(index));
	}
	
	@Test
	public void testBoolean() {
		final Boolean flinkTuple = new Boolean(this.r.nextBoolean());
		
		final StormTuple<Boolean> tuple = new StormTuple<Boolean>(flinkTuple);
		Assert.assertEquals(flinkTuple, tuple.getBoolean(0));
	}
	
	@Test
	public void testByte() {
		final Byte flinkTuple = new Byte((byte)this.r.nextInt());
		
		final StormTuple<Byte> tuple = new StormTuple<Byte>(flinkTuple);
		Assert.assertEquals(flinkTuple, tuple.getByte(0));
	}
	
	@Test
	public void testDouble() {
		final Double flinkTuple = new Double(this.r.nextDouble());
		
		final StormTuple<Double> tuple = new StormTuple<Double>(flinkTuple);
		Assert.assertEquals(flinkTuple, tuple.getDouble(0));
	}
	
	@Test
	public void testFloat() {
		final Float flinkTuple = new Float(this.r.nextFloat());
		
		final StormTuple<Float> tuple = new StormTuple<Float>(flinkTuple);
		Assert.assertEquals(flinkTuple, tuple.getFloat(0));
	}
	
	@Test
	public void testInteger() {
		final Integer flinkTuple = new Integer(this.r.nextInt());
		
		final StormTuple<Integer> tuple = new StormTuple<Integer>(flinkTuple);
		Assert.assertEquals(flinkTuple, tuple.getInteger(0));
	}
	
	@Test
	public void testLong() {
		final Long flinkTuple = new Long(this.r.nextInt());
		
		final StormTuple<Long> tuple = new StormTuple<Long>(flinkTuple);
		Assert.assertEquals(flinkTuple, tuple.getLong(0));
	}
	
	@Test
	public void testShort() {
		final Short flinkTuple = new Short((short)this.r.nextInt());
		
		final StormTuple<Short> tuple = new StormTuple<Short>(flinkTuple);
		Assert.assertEquals(flinkTuple, tuple.getShort(0));
	}
	
	@Test
	public void testString() {
		final byte[] data = new byte[this.r.nextInt(15)];
		this.r.nextBytes(data);
		final String flinkTuple = new String(data);
		
		final StormTuple<String> tuple = new StormTuple<String>(flinkTuple);
		Assert.assertEquals(flinkTuple, tuple.getString(0));
	}
	
	@Test
	public void testBinaryTuple() {
		final byte[] data = new byte[this.r.nextInt(15)];
		this.r.nextBytes(data);
		
		final int index = this.r.nextInt(5);
		final Tuple flinkTuple = new Tuple5<Object, Object, Object, Object, Object>();
		flinkTuple.setField(data, index);
		
		final StormTuple<Tuple> tuple = new StormTuple<Tuple>(flinkTuple);
		Assert.assertEquals(flinkTuple.getField(index), tuple.getBinary(index));
	}
	
	@Test
	public void testBooleanTuple() {
		final Boolean data = new Boolean(this.r.nextBoolean());
		
		final int index = this.r.nextInt(5);
		final Tuple flinkTuple = new Tuple5<Object, Object, Object, Object, Object>();
		flinkTuple.setField(data, index);
		
		final StormTuple<Tuple> tuple = new StormTuple<Tuple>(flinkTuple);
		Assert.assertEquals(flinkTuple.getField(index), tuple.getBoolean(index));
	}
	
	@Test
	public void testByteTuple() {
		final Byte data = new Byte((byte)this.r.nextInt());
		
		final int index = this.r.nextInt(5);
		final Tuple flinkTuple = new Tuple5<Object, Object, Object, Object, Object>();
		flinkTuple.setField(data, index);
		
		final StormTuple<Tuple> tuple = new StormTuple<Tuple>(flinkTuple);
		Assert.assertEquals(flinkTuple.getField(index), tuple.getByte(index));
	}
	
	@Test
	public void testDoubleTuple() {
		final Double data = new Double(this.r.nextDouble());
		
		final int index = this.r.nextInt(5);
		final Tuple flinkTuple = new Tuple5<Object, Object, Object, Object, Object>();
		flinkTuple.setField(data, index);
		
		final StormTuple<Tuple> tuple = new StormTuple<Tuple>(flinkTuple);
		Assert.assertEquals(flinkTuple.getField(index), tuple.getDouble(index));
	}
	
	@Test
	public void testFloatTuple() {
		final Float data = new Float(this.r.nextFloat());
		
		final int index = this.r.nextInt(5);
		final Tuple flinkTuple = new Tuple5<Object, Object, Object, Object, Object>();
		flinkTuple.setField(data, index);
		
		final StormTuple<Tuple> tuple = new StormTuple<Tuple>(flinkTuple);
		Assert.assertEquals(flinkTuple.getField(index), tuple.getFloat(index));
	}
	
	@Test
	public void testIntegerTuple() {
		final Integer data = new Integer(this.r.nextInt());
		
		final int index = this.r.nextInt(5);
		final Tuple flinkTuple = new Tuple5<Object, Object, Object, Object, Object>();
		flinkTuple.setField(data, index);
		
		final StormTuple<Tuple> tuple = new StormTuple<Tuple>(flinkTuple);
		Assert.assertEquals(flinkTuple.getField(index), tuple.getInteger(index));
	}
	
	@Test
	public void testLongTuple() {
		final Long data = new Long(this.r.nextInt());
		
		final int index = this.r.nextInt(5);
		final Tuple flinkTuple = new Tuple5<Object, Object, Object, Object, Object>();
		flinkTuple.setField(data, index);
		
		final StormTuple<Tuple> tuple = new StormTuple<Tuple>(flinkTuple);
		Assert.assertEquals(flinkTuple.getField(index), tuple.getLong(index));
	}
	
	@Test
	public void testShortTuple() {
		final Short data = new Short((short)this.r.nextInt());
		
		final int index = this.r.nextInt(5);
		final Tuple flinkTuple = new Tuple5<Object, Object, Object, Object, Object>();
		flinkTuple.setField(data, index);
		
		final StormTuple<Tuple> tuple = new StormTuple<Tuple>(flinkTuple);
		Assert.assertEquals(flinkTuple.getField(index), tuple.getShort(index));
	}
	
	@Test
	public void testStringTuple() {
		final byte[] rawdata = new byte[this.r.nextInt(15)];
		this.r.nextBytes(rawdata);
		final String data = new String(rawdata);
		
		final int index = this.r.nextInt(5);
		final Tuple flinkTuple = new Tuple5<Object, Object, Object, Object, Object>();
		flinkTuple.setField(data, index);
		
		final StormTuple<Tuple> tuple = new StormTuple<Tuple>(flinkTuple);
		Assert.assertEquals(flinkTuple.getField(index), tuple.getString(index));
	}
	
	@Test(expected = UnsupportedOperationException.class)
	public void testContains() {
		new StormTuple<Object>(null).contains(null);
	}
	
	@Test(expected = UnsupportedOperationException.class)
	public void testGetFields() {
		new StormTuple<Object>(null).getFields();
	}
	
	@Test(expected = UnsupportedOperationException.class)
	public void testFieldIndex() {
		new StormTuple<Object>(null).fieldIndex(null);
	}
	
	@Test(expected = UnsupportedOperationException.class)
	public void testSelect() {
		new StormTuple<Object>(null).select(null);
	}
	
	@Test(expected = UnsupportedOperationException.class)
	public void testGetValueByField() {
		new StormTuple<Object>(null).getValueByField(null);
	}
	
	@Test(expected = UnsupportedOperationException.class)
	public void testGetStringByField() {
		new StormTuple<Object>(null).getStringByField(null);
	}
	
	@Test(expected = UnsupportedOperationException.class)
	public void testGetIntegerByField() {
		new StormTuple<Object>(null).getIntegerByField(null);
	}
	
	@Test(expected = UnsupportedOperationException.class)
	public void testGetLongByField() {
		new StormTuple<Object>(null).getLongByField(null);
	}
	
	@Test(expected = UnsupportedOperationException.class)
	public void testGetBooleanByField() {
		new StormTuple<Object>(null).getBooleanByField(null);
	}
	
	@Test(expected = UnsupportedOperationException.class)
	public void testGetShortByField() {
		new StormTuple<Object>(null).getShortByField(null);
	}
	
	@Test(expected = UnsupportedOperationException.class)
	public void testGetByteByField() {
		new StormTuple<Object>(null).getByteByField(null);
	}
	
	@Test(expected = UnsupportedOperationException.class)
	public void testGetDoubleByField() {
		new StormTuple<Object>(null).getDoubleByField(null);
	}
	
	@Test(expected = UnsupportedOperationException.class)
	public void testGetFloatByField() {
		new StormTuple<Object>(null).getFloatByField(null);
	}
	
	@Test(expected = UnsupportedOperationException.class)
	public void testGetBinaryByField() {
		new StormTuple<Object>(null).getBinaryByField(null);
	}
	
	@Test(expected = UnsupportedOperationException.class)
	public void testGetSourceGlobalStreamid() {
		new StormTuple<Object>(null).getSourceGlobalStreamid();
	}
	
	@Test(expected = UnsupportedOperationException.class)
	public void testGetSourceComponent() {
		new StormTuple<Object>(null).getSourceComponent();
	}
	
	@Test(expected = UnsupportedOperationException.class)
	public void testGetSourceTask() {
		new StormTuple<Object>(null).getSourceTask();
	}
	
	@Test(expected = UnsupportedOperationException.class)
	public void testGetSourceStreamId() {
		new StormTuple<Object>(null).getSourceStreamId();
	}
	
	@Test(expected = UnsupportedOperationException.class)
	public void testGetMessageId() {
		new StormTuple<Object>(null).getMessageId();
	}
	
}
