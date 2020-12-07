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

package org.apache.flink.runtime.messages;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.core.testutils.CommonTestUtils;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.ParameterizedType;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class GenericMessageTester {

	public static void testMessageInstance(Serializable instance) throws Exception {
		Serializable copy = CommonTestUtils.createCopySerializable(instance);
		
		// test equals, hash code, toString
		assertTrue(instance.equals(copy));
		assertTrue(copy.equals(instance));
		assertTrue(instance.hashCode() == copy.hashCode());
		assertTrue(instance.toString().equals(copy.toString()));
	}
	
	public static void testMessageInstances(Serializable instance1, Serializable instance2) throws Exception {
		// test equals, hash code, toString
		assertTrue(instance1.equals(instance2));
		assertTrue(instance2.equals(instance1));
		assertTrue(instance1.hashCode() == instance2.hashCode());
		assertTrue(instance1.toString().equals(instance2.toString()));

		// test serializability
		Serializable copy = CommonTestUtils.createCopySerializable(instance1);
		assertTrue(instance1.equals(copy));
		assertTrue(copy.equals(instance1));
		assertTrue(instance1.hashCode() == copy.hashCode());
	}

	// ------------------------------------------------------------------------
	//  Random Generators
	// ------------------------------------------------------------------------
	
	@SuppressWarnings("unchecked")
	public static <T> T instantiateGeneric(Class<T> messageClass, Random rnd, Instantiator<?>... extraInstantiators) {
		try {
			// build the map of extra instantiators
			Map<Class<?>, Instantiator<?>> extraInsts = new HashMap<>();
			for (Instantiator<?> inst : extraInstantiators) {
				Class<?> type = (Class<?>) ((ParameterizedType) inst.getClass().getGenericInterfaces()[0]).getActualTypeArguments()[0];
				assertNotNull("Cannot get type for extra instantiator", type);
				extraInsts.put(type, inst);
			}

			Constructor<?>[] constructors = messageClass.getConstructors();

			Class<?> missingType = null;
			
			outer:
			for (Constructor<?> constructor : constructors) {
				
				Class<?>[] paramTypes = constructor.getParameterTypes();
				Object[] params = new Object[paramTypes.length];

				for (int i = 0; i < paramTypes.length; i++) {
					Instantiator<?> inst = extraInsts.get(paramTypes[i]);
					if (inst == null) {
						inst = INSTANTIATORS.get(paramTypes[i]);
					}
					
					if (inst == null) {
						missingType = paramTypes[i];
						continue outer;
					}
					
					params[i] = inst.instantiate(rnd);
				}

				return (T) constructor.newInstance(params);
			}

			//noinspection ConstantConditions
			fail("No instantiator available for type " + missingType.getCanonicalName());
			throw new RuntimeException();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail("Could not perform reflective tests: " + e.getMessage());
			throw new RuntimeException();
		}
	}
	
	public static String randomString(Random rnd) {
		int len = rnd.nextInt(64 + 1);
		char[] chars = new char[len];
		for (int i = 0; i < len; i++) {
			chars[i] = (char) rnd.nextInt();
		}
		return new String(chars);
	}
	
	public static JobID randomJobId(Random rnd) {
		return new JobID(rnd.nextLong(), rnd.nextLong());
	}
	
	public static JobStatus randomJobStatus(Random rnd) {
		return JobStatus.values()[rnd.nextInt(JobStatus.values().length)];
	}

	// ------------------------------------------------------------------------
	//  Map of Instantiators
	// ------------------------------------------------------------------------
	
	private static final Map<Class<?>, Instantiator<?>> INSTANTIATORS = new HashMap<>();
	
	static {
		INSTANTIATORS.put(boolean.class, new BooleanInstantiator());
		INSTANTIATORS.put(Boolean.class, new BooleanInstantiator());

		INSTANTIATORS.put(char.class, new CharInstantiator());
		INSTANTIATORS.put(Character.class, new CharInstantiator());

		INSTANTIATORS.put(byte.class, new ByteInstantiator());
		INSTANTIATORS.put(Byte.class, new ByteInstantiator());

		INSTANTIATORS.put(short.class, new ShortInstantiator());
		INSTANTIATORS.put(Short.class, new ShortInstantiator());

		INSTANTIATORS.put(int.class, new IntInstantiator());
		INSTANTIATORS.put(Integer.class, new IntInstantiator());

		INSTANTIATORS.put(long.class, new LongInstantiator());
		INSTANTIATORS.put(Long.class, new LongInstantiator());

		INSTANTIATORS.put(float.class, new FloatInstantiator());
		INSTANTIATORS.put(Float.class, new FloatInstantiator());

		INSTANTIATORS.put(double.class, new DoubleInstantiator());
		INSTANTIATORS.put(Double.class, new DoubleInstantiator());

		INSTANTIATORS.put(String.class, new StringInstantiator());

		INSTANTIATORS.put(JobID.class, new JobIdInstantiator());
		INSTANTIATORS.put(JobStatus.class, new JobStatusInstantiator());
	}
	
	// ------------------------------------------------------------------------
	//  Instantiators
	// ------------------------------------------------------------------------
	
	public static interface Instantiator<T> {
		
		T instantiate(Random rnd);
	}

	public static class ByteInstantiator implements Instantiator<Byte> {

		@Override
		public Byte instantiate(Random rnd) {
			int i = rnd.nextInt(100);
			return (byte) i;
		}
	}

	public static class ShortInstantiator implements Instantiator<Short> {

		@Override
		public Short instantiate(Random rnd) {
			return (short) rnd.nextInt(30000);
		}
	}

	public static class IntInstantiator implements Instantiator<Integer> {

		@Override
		public Integer instantiate(Random rnd) {
			return rnd.nextInt(Integer.MAX_VALUE);
		}
	}

	public static class LongInstantiator implements Instantiator<Long> {

		@Override
		public Long instantiate(Random rnd) {
			return (long) rnd.nextInt(Integer.MAX_VALUE);
		}
	}

	public static class FloatInstantiator implements Instantiator<Float> {

		@Override
		public Float instantiate(Random rnd) {
			return rnd.nextFloat();
		}
	}

	public static class DoubleInstantiator implements Instantiator<Double> {

		@Override
		public Double instantiate(Random rnd) {
			return rnd.nextDouble();
		}
	}

	public static class BooleanInstantiator implements Instantiator<Boolean> {

		@Override
		public Boolean instantiate(Random rnd) {
			return rnd.nextBoolean();
		}
	}

	public static class CharInstantiator implements Instantiator<Character> {

		@Override
		public Character instantiate(Random rnd) {
			return (char) rnd.nextInt(30000);
		}
	}

	public static class StringInstantiator implements Instantiator<String> {

		@Override
		public String instantiate(Random rnd) {
			return randomString(rnd);
		}
	}

	public static class JobIdInstantiator implements Instantiator<JobID> {

		@Override
		public JobID instantiate(Random rnd) {
			return randomJobId(rnd);
		}
	}

	public static class JobStatusInstantiator implements Instantiator<JobStatus> {

		@Override
		public JobStatus instantiate(Random rnd) {
			return randomJobStatus(rnd);
		}
	}
}
