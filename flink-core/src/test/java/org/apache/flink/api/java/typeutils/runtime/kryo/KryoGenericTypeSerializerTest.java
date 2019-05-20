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

package org.apache.flink.api.java.typeutils.runtime.kryo;

import com.esotericsoftware.kryo.Kryo;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.ComparatorTestBase;
import org.apache.flink.api.java.typeutils.runtime.AbstractGenericTypeSerializerTest;
import org.apache.flink.api.java.typeutils.runtime.TestDataOutputSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Random;

import static org.junit.Assert.*;

@SuppressWarnings("unchecked")
public class KryoGenericTypeSerializerTest extends AbstractGenericTypeSerializerTest {

	ExecutionConfig ec = new ExecutionConfig();
	
	@Test
	public void testJavaList(){
		Collection<Integer> a = new ArrayList<>();

		fillCollection(a);

		runTests(a);
	}

	@Test
	public void testJavaSet(){
		Collection<Integer> b = new HashSet<>();

		fillCollection(b);

		runTests(b);
	}



	@Test
	public void testJavaDequeue(){
		Collection<Integer> c = new LinkedList<>();
		fillCollection(c);
		runTests(c);
	}

	private void fillCollection(Collection<Integer> coll) {
		coll.add(42);
		coll.add(1337);
		coll.add(49);
		coll.add(1);
	}

	@Override
	protected <T> TypeSerializer<T> createSerializer(Class<T> type) {
		return new KryoSerializer<T>(type, ec);
	}
	
	/**
	 * Make sure that the kryo serializer forwards EOF exceptions properly when serializing
	 */
	@Test
	public void testForwardEOFExceptionWhileSerializing() {
		try {
			// construct a long string
			String str;
			{
				char[] charData = new char[40000];
				Random rnd = new Random();
				
				for (int i = 0; i < charData.length; i++) {
					charData[i] = (char) rnd.nextInt(10000);
				}
				
				str = new String(charData);
			}
			
			// construct a memory target that is too small for the string
			TestDataOutputSerializer target = new TestDataOutputSerializer(10000, 30000);
			KryoSerializer<String> serializer = new KryoSerializer<String>(String.class, new ExecutionConfig());
			
			try {
				serializer.serialize(str, target);
				fail("should throw a java.io.EOFException");
			}
			catch (java.io.EOFException e) {
				// that is how we like it
			}
			catch (Exception e) {
				fail("throws wrong exception: should throw a java.io.EOFException, has thrown a " + e.getClass().getName());
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	/**
	 * Make sure that the kryo serializer forwards EOF exceptions properly when serializing
	 */
	@Test
	public void testForwardEOFExceptionWhileDeserializing() {
		try {
			int numElements = 100;
			// construct a memory target that is too small for the string
			TestDataOutputSerializer target = new TestDataOutputSerializer(5*numElements, 5*numElements);
			KryoSerializer<Integer> serializer = new KryoSerializer<>(Integer.class, new ExecutionConfig());

			for(int i = 0; i < numElements; i++){
				serializer.serialize(i, target);
			}

			ComparatorTestBase.TestInputView source = new ComparatorTestBase.TestInputView(target.copyByteBuffer());

			for(int i = 0; i < numElements; i++){
				int value = serializer.deserialize(source);
				assertEquals(i, value);
			}

			try {
				serializer.deserialize(source);
				fail("should throw a java.io.EOFException");
			}
			catch (java.io.EOFException e) {
				// that is how we like it :-)
			}
			catch (Exception e) {
				fail("throws wrong exception: should throw a java.io.EOFException, has thrown a " + e.getClass().getName());
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void validateReferenceMappingEnabled() {
		KryoSerializer<String> serializer = new KryoSerializer<>(String.class, new ExecutionConfig());
		Kryo kryo = serializer.getKryo();
		assertTrue(kryo.getReferences());
	}
}
