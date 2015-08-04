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
package org.apache.flink.streaming.connectors.kafka.util;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.kafka.Utils;
import org.junit.Assert;
import org.junit.Test;

public class UtilsTest {

	/**
	 * Ensure that the returned byte array has the expected size
	 */
	@Test
	public void testTypeInformationSerializationSchema() {
		final ExecutionConfig ec = new ExecutionConfig();

		Tuple2<Integer, Integer> test = new Tuple2<Integer, Integer>(1,666);

		Utils.TypeInformationSerializationSchema<Tuple2<Integer, Integer>> ser = new Utils.TypeInformationSerializationSchema<Tuple2<Integer, Integer>>(test, ec);

		byte[] res = ser.serialize(test);
		Assert.assertEquals(8, res.length);

		Tuple2<Integer, Integer> another = ser.deserialize(res);
		Assert.assertEquals(test.f0, another.f0);
		Assert.assertEquals(test.f1, another.f1);
	}

	@Test
	public void testGrowing() {
		final ExecutionConfig ec = new ExecutionConfig();

		Tuple2<Integer, byte[]> test1 = new Tuple2<Integer, byte[]>(1, new byte[16]);

		Utils.TypeInformationSerializationSchema<Tuple2<Integer, byte[]>> ser = new Utils.TypeInformationSerializationSchema<Tuple2<Integer, byte[]>>(test1, ec);

		byte[] res = ser.serialize(test1);
		Assert.assertEquals(24, res.length);
		Tuple2<Integer, byte[]> another = ser.deserialize(res);
		Assert.assertEquals(16, another.f1.length);

		test1 = new Tuple2<Integer, byte[]>(1, new byte[26]);

		res = ser.serialize(test1);
		Assert.assertEquals(34, res.length);
		another = ser.deserialize(res);
		Assert.assertEquals(26, another.f1.length);

		test1 = new Tuple2<Integer, byte[]>(1, new byte[1]);

		res = ser.serialize(test1);
		Assert.assertEquals(9, res.length);
		another = ser.deserialize(res);
		Assert.assertEquals(1, another.f1.length);
	}


}
