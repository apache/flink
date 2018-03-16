/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.java.functions;

import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ClosureCleaner;

import org.junit.Assert;
import org.junit.Test;

import java.io.Serializable;

/**
 * Tests for {@link ClosureCleaner}.
 */
public class ClosureCleanerTest {

	@Test(expected = InvalidProgramException.class)
	public void testNonSerializable() throws Exception  {
		MapCreator creator = new NonSerializableMapCreator();
		MapFunction<Integer, Integer> map = creator.getMap();

		ClosureCleaner.ensureSerializable(map);

		int result = map.map(3);
		Assert.assertEquals(result, 4);
	}

	@Test
	public void testCleanedNonSerializable() throws Exception  {
		MapCreator creator = new NonSerializableMapCreator();
		MapFunction<Integer, Integer> map = creator.getMap();

		ClosureCleaner.clean(map, true);

		int result = map.map(3);
		Assert.assertEquals(result, 4);
	}

	@Test
	public void testSerializable() throws Exception  {
		MapCreator creator = new SerializableMapCreator(1);
		MapFunction<Integer, Integer> map = creator.getMap();

		ClosureCleaner.clean(map, true);

		int result = map.map(3);
		Assert.assertEquals(result, 4);
	}

	@Test
	public void testNestedSerializable() throws Exception  {
		MapCreator creator = new NestedSerializableMapCreator(1);
		MapFunction<Integer, Integer> map = creator.getMap();

		ClosureCleaner.clean(map, true);

		ClosureCleaner.ensureSerializable(map);

		int result = map.map(3);
		Assert.assertEquals(result, 4);
	}

	@Test(expected = InvalidProgramException.class)
	public void testNestedNonSerializable() throws Exception  {
		MapCreator creator = new NestedNonSerializableMapCreator(1);
		MapFunction<Integer, Integer> map = creator.getMap();

		ClosureCleaner.clean(map, true);

		ClosureCleaner.ensureSerializable(map);

		int result = map.map(3);
		Assert.assertEquals(result, 4);
	}
}

interface MapCreator {
	MapFunction<Integer, Integer> getMap();
}

@SuppressWarnings("serial")
class NonSerializableMapCreator implements MapCreator {

	@Override
	public MapFunction<Integer, Integer> getMap() {
		return new MapFunction<Integer, Integer>() {
			@Override
			public Integer map(Integer value) throws Exception {
				return value + 1;
			}
		};
	}

}

@SuppressWarnings("serial")
class SerializableMapCreator implements MapCreator, Serializable {

	private int add = 0;

	public SerializableMapCreator(int add) {
		this.add = add;
	}

	@Override
	public MapFunction<Integer, Integer> getMap() {
		return new MapFunction<Integer, Integer>() {
			@Override
			public Integer map(Integer value) throws Exception {
				return value + add;
			}
		};
	}

}

@SuppressWarnings("serial")
class NestedSerializableMapCreator implements MapCreator, Serializable {

	private int add = 0;
	private InnerSerializableMapCreator inner;

	public NestedSerializableMapCreator(int add) {
		this.add = add;
		this.inner = new InnerSerializableMapCreator();
	}

	@Override
	public MapFunction<Integer, Integer> getMap() {
		return inner.getMap();
	}

	class InnerSerializableMapCreator implements MapCreator, Serializable {

		@Override
		public MapFunction<Integer, Integer> getMap() {
			return new MapFunction<Integer, Integer>() {
				@Override
				public Integer map(Integer value) throws Exception {
					return value + add;
				}
			};
		}

	}

}

class NestedNonSerializableMapCreator implements MapCreator {

	private int add = 0;
	private InnerSerializableMapCreator inner;

	public NestedNonSerializableMapCreator(int add) {
		this.add = add;
		this.inner = new InnerSerializableMapCreator();
	}

	@Override
	public MapFunction<Integer, Integer> getMap() {
		return inner.getMap();
	}

	@SuppressWarnings("serial")
	class InnerSerializableMapCreator implements MapCreator, Serializable {

		@Override
		public MapFunction<Integer, Integer> getMap() {
			return new MapFunction<Integer, Integer>() {
				@Override
				public Integer map(Integer value) throws Exception {
					return value + getMeTheAdd();
				}
			};
		}

		public int getMeTheAdd() {
			return add;
		}

	}

}

