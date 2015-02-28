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

package org.apache.flink.streaming.connectors.kafka;

import static org.junit.Assert.assertEquals;

import java.io.Serializable;

import org.apache.flink.streaming.connectors.kafka.config.StringSerializer;
import org.junit.Test;

public class StringSerializerTest {

	private static class MyClass implements Serializable {

		private static final long serialVersionUID = 1L;
		private int a;
		private String b;

		public MyClass(int a, String b) {
			this.a = a;
			this.b = b;
		}

		@Override
		public boolean equals(Object o) {
			if (o == null) {
				return false;
			} else {
				try {
					MyClass other = (MyClass) o;
					return a == other.a && b.equals(other.b);
				} catch (ClassCastException e) {
					return false;
				}
			}
		}
	}

	@Test
	public void test() {

		MyClass myObject = new MyClass(42, "test string");

		StringSerializer<MyClass> stringSerializer = new StringSerializer<MyClass>();

		String store = stringSerializer.serialize(myObject);

		MyClass myObjectDeserialized = stringSerializer.deserialize(store);

		assertEquals(myObject, myObjectDeserialized);
	}

}
