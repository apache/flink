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

package org.apache.flink.streaming.util;

import static org.junit.Assert.assertEquals;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.util.keys.FieldsKeySelector;
import org.apache.flink.streaming.util.keys.ObjectKeySelector;
import org.apache.flink.streaming.util.keys.TupleKeySelector;
import org.junit.Test;

public class FieldsKeySelectorTest {

	@Test
	public void testGetKey() throws Exception {

		Integer i = 5;
		Tuple2<Integer, String> t = new Tuple2<Integer, String>(-1, "a");
		double[] a = new double[] { 0.0, 1.2 };

		KeySelector<Integer, ?> ks1 = new ObjectKeySelector<Integer>();

		assertEquals(ks1.getKey(i), 5);

		KeySelector<Tuple2<Integer, String>, ?> ks3 = new TupleKeySelector<Tuple2<Integer, String>>(
				1);
		assertEquals(ks3.getKey(t), "a");

		KeySelector<Tuple2<Integer, String>, ?> ks4 = FieldsKeySelector.getSelector(
				TypeExtractor.getForObject(t), 1, 1);
		assertEquals(ks4.getKey(t), new Tuple2<String, String>("a", "a"));

		KeySelector<double[], ?> ks5 = FieldsKeySelector.getSelector(
				TypeExtractor.getForObject(a), 0);
		assertEquals(ks5.getKey(a), 0.0);

		KeySelector<double[], ?> ks6 = FieldsKeySelector.getSelector(
				TypeExtractor.getForObject(a), 1, 0);
		assertEquals(ks6.getKey(a), new Tuple2<Double, Double>(1.2, 0.0));

	}

}
