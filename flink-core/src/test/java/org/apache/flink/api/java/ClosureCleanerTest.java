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

package org.apache.flink.api.java;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.util.function.SerializableSupplier;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
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

		ClosureCleaner.clean(map, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);

		int result = map.map(3);
		Assert.assertEquals(result, 4);
	}

	@Test
	public void testSerializable() throws Exception  {
		MapCreator creator = new SerializableMapCreator(1);
		MapFunction<Integer, Integer> map = creator.getMap();

		ClosureCleaner.clean(map, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);

		int result = map.map(3);
		Assert.assertEquals(result, 4);
	}

	@Test
	public void testNestedSerializable() throws Exception  {
		MapCreator creator = new NestedSerializableMapCreator(1);
		MapFunction<Integer, Integer> map = creator.getMap();

		ClosureCleaner.clean(map, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);

		ClosureCleaner.ensureSerializable(map);

		int result = map.map(3);
		Assert.assertEquals(result, 4);
	}

	@Test(expected = InvalidProgramException.class)
	public void testNestedNonSerializable() throws Exception  {
		MapCreator creator = new NestedNonSerializableMapCreator(1);
		MapFunction<Integer, Integer> map = creator.getMap();

		ClosureCleaner.clean(map, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);

		ClosureCleaner.ensureSerializable(map);

		int result = map.map(3);
		Assert.assertEquals(result, 4);
	}

	@Test
	public void testWrapperClass() throws Exception {
		MapCreator creator = new NonSerializableMapCreator();
		MapFunction<Integer, Integer> notCleanedMap = creator.getMap();

		WrapperMapFunction wrapped = new WrapperMapFunction(notCleanedMap);

		ClosureCleaner.clean(wrapped, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);

		ClosureCleaner.ensureSerializable(wrapped);

		int result = wrapped.map(3);
		Assert.assertEquals(result, 4);
	}

	@Test
	public void testComplexTopLevelClassClean() throws Exception {
		MapFunction<Integer, Integer> complexMap = new ComplexMap((MapFunction<Integer, Integer>) value -> value + 1);

		ClosureCleaner.clean(complexMap, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);

		int result = complexMap.map(3);

		Assert.assertEquals(result, 5);
	}

	@Test
	public void testComplexInnerClassClean() throws Exception {
		MapFunction<Integer, Integer> complexMap = new InnerComplexMap((MapFunction<Integer, Integer>) value -> value + 1);

		ClosureCleaner.clean(complexMap, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);

		int result = complexMap.map(3);

		Assert.assertEquals(result, 4);
	}

	@Test
	public void testSelfReferencingClean() {
		final NestedSelfReferencing selfReferencing = new NestedSelfReferencing();
		ClosureCleaner.clean(selfReferencing, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);
	}

	class InnerCustomMap implements MapFunction<Integer, Integer> {

		@Override
		public Integer map(Integer value) throws Exception {
			return value + 1;
		}

		private void writeObject(ObjectOutputStream o) throws IOException {
		}

		private void readObject(ObjectInputStream i) throws IOException {
		}
	}

	// Inner class
	class InnerComplexMap implements MapFunction<Integer, Integer> {

		InnerCustomMap map1;
		LocalMap map3;

		class LocalMap implements MapFunction<Integer, Integer> {

			MapFunction<Integer, Integer> map2;

			public LocalMap(MapFunction<Integer, Integer> map2) {
				this.map2 = map2;
			}

			@Override
			public Integer map(Integer value) throws Exception {
				return map2.map(value + 1);
			}
		}

		public InnerComplexMap(MapFunction<Integer, Integer> map) {
			this.map1 = new InnerCustomMap();
			this.map3 = new LocalMap(map);
		}

		@Override
		public Integer map(Integer value) throws Exception {
			return map1.map(value);
		}
	}

	@Test
	public void testOuterStaticClassInnerStaticClassInnerAnonymousOrLocalClass() {
		MapFunction<Integer, Integer> nestedMap = new OuterMapCreator().getMap();

		MapFunction<Integer, Integer> wrappedMap = new WrapperMapFunction(nestedMap);

		ClosureCleaner.clean(wrappedMap, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);

		ClosureCleaner.ensureSerializable(wrappedMap);
	}

	@Test
	public void testRealOuterStaticClassInnerStaticClassInnerAnonymousOrLocalClass() {
		MapFunction<Integer, Integer> nestedMap = new OuterMapCreator().getMap();

		MapFunction<Integer, Integer> wrappedMap = new WrapperMapFunction(nestedMap);

		Tuple1<MapFunction<Integer, Integer>> tuple = new Tuple1<>(wrappedMap);

		ClosureCleaner.clean(tuple, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);

		ClosureCleaner.ensureSerializable(tuple);
	}

	@Test
	public void testRecursiveClass() {
		RecursiveClass recursiveClass = new RecursiveClass(new RecursiveClass());

		ClosureCleaner.clean(recursiveClass, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);

		ClosureCleaner.ensureSerializable(recursiveClass);
	}

	@Test
	public void testWriteReplace() {
		WithWriteReplace.SerializablePayload writeReplace =
			new WithWriteReplace.SerializablePayload(new WithWriteReplace.Payload("text"));
		Assert.assertEquals("text", writeReplace.get().getRaw());
		ClosureCleaner.clean(writeReplace, ExecutionConfig.ClosureCleanerLevel.TOP_LEVEL, true);
	}

	@Test
	public void testWriteReplaceRecursive() {
		WithWriteReplace writeReplace = new WithWriteReplace(new WithWriteReplace.Payload("text"));
		Assert.assertEquals("text", writeReplace.getPayload().getRaw());
		ClosureCleaner.clean(writeReplace, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);
	}

	/**
	 * Verify that ClosureCleaner works correctly on Object, which doesn't have any superclasses
	 * or interfaces that it implements.
	 */
	@Test(expected = InvalidProgramException.class)
	public void testCleanObject() {
		ClosureCleaner.clean(new Object(), ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);
	}
}

class CustomMap implements MapFunction<Integer, Integer> {

	@Override
	public Integer map(Integer value) throws Exception {
		return value + 1;
	}

	public void writeObject(ObjectOutputStream o) {

	}
}

// top level class
class ComplexMap implements MapFunction<Integer, Integer> {

	private static MapFunction<Integer, Integer> map1;

	private transient MapFunction<Integer, Integer> map2;
	private CustomMap map3;
	private LocalMap map4;

	class LocalMap implements MapFunction<Integer, Integer> {

		MapFunction<Integer, Integer> map4;

		public LocalMap(MapFunction<Integer, Integer> map4) {
			this.map4 = map4;
		}

		@Override
		public Integer map(Integer value) throws Exception {
			return map4.map(value + 1);
		}
	}

	ComplexMap(MapFunction<Integer, Integer> map2) {
		map1 = map2;
		this.map2 = map2;
		this.map3 = new CustomMap();
		this.map4 = new LocalMap(map2);
	}

	@Override
	public Integer map(Integer value) throws Exception {
		return map4.map(value);
	}
}

class RecursiveClass implements Serializable {

	private RecursiveClass recurse;

	RecursiveClass() {
	}

	RecursiveClass(RecursiveClass recurse) {
		this.recurse = recurse;
	}
}

interface MapCreator {
	MapFunction<Integer, Integer> getMap();
}

class WrapperMapFunction implements MapFunction<Integer, Integer> {

	private MapFunction<Integer, Integer> innerMapFuc;

	WrapperMapFunction(MapFunction<Integer, Integer> mapFunction) {
		innerMapFuc = mapFunction;
	}

	@Override
	public Integer map(Integer value) throws Exception {
		return innerMapFuc.map(value);
	}
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

	SerializableMapCreator(int add) {
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

	NestedSerializableMapCreator(int add) {
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

	NestedNonSerializableMapCreator(int add) {
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

class OuterMapCreator implements MapCreator {

	static class OuterStaticClass implements MapCreator {

		static class InnerStaticClass implements MapCreator{

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

		@Override
		public MapFunction<Integer, Integer> getMap() {
			InnerStaticClass innerClass = new InnerStaticClass();
			return innerClass.getMap();
		}
	}

	@Override
	public MapFunction<Integer, Integer> getMap() {
		return new OuterStaticClass().getMap();
	}
}

class NestedSelfReferencing implements Serializable {

	private final SerializableSupplier<NestedSelfReferencing> cycle;

	NestedSelfReferencing() {
		this.cycle = () -> this;
	}

	public SerializableSupplier<NestedSelfReferencing> getCycle() {
		return cycle;
	}
}

class WithWriteReplace implements Serializable {

	private final SerializablePayload serializablePayload;

	WithWriteReplace(Payload payload) {
		this.serializablePayload = new SerializablePayload(payload);
	}

	Payload getPayload() {
		return serializablePayload.get();
	}

	static class Payload {

		private final String raw;

		Payload(String raw) {
			this.raw = raw;
		}

		String getRaw() {
			return raw;
		}
	}

	static class SerializablePayload implements Serializable {

		private final Payload payload;

		SerializablePayload(Payload payload) {
			this.payload = payload;
		}

		private Object writeReplace() {
			return new SerializedPayload(payload.getRaw());
		}

		Payload get() {
			return payload;
		}
	}

	private static class SerializedPayload implements Serializable {

		private final String raw;

		private SerializedPayload(String raw) {
			this.raw = raw;
		}

		private Object readResolve() throws IOException, ClassNotFoundException {
			return new Payload(raw);
		}
	}

}

