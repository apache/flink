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

package org.apache.flink.formats.avro;

import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.core.testutils.FilteredClassLoader;
import org.apache.flink.formats.avro.utils.AvroKryoSerializerUtils;
import org.apache.flink.runtime.execution.librarycache.FlinkUserCodeClassLoaders;

import com.esotericsoftware.kryo.Kryo;
import org.junit.Test;

import java.lang.reflect.Method;
import java.net.URL;
import java.util.LinkedHashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * This test makes sure that reversed classloading works for the Avro/Kryo integration when
 * Kryo is in the application jar file.
 *
 * <p>If Kryo is not loaded consistently through the same classloader (parent-first), the following
 * error happens:
 *
 * <pre>
 * java.lang.VerifyError: Bad type on operand stack
 * Exception Details:
 *   Location:
 *  org/apache/flink/formats/avro/utils/AvroKryoSerializerUtils.addAvroGenericDataArrayRegistration(Ljava/util/LinkedHashMap;)V @23: invokespecial
 *   Reason:
 *     Type 'org/apache/flink/api/java/typeutils/runtime/kryo/Serializers$SpecificInstanceCollectionSerializerForArrayList' (current frame, stack[7]) is not assignable to 'com/esotericsoftware/kryo/Serializer'
 *   Current Frame:
 *     bci: @23
 *     flags: { }
 *     locals: { 'org/apache/flink/formats/avro/utils/AvroKryoSerializerUtils', 'java/util/LinkedHashMap' }
 *     stack: { 'java/util/LinkedHashMap', 'java/lang/String', uninitialized 6, uninitialized 6, 'java/lang/Class', uninitialized 12, uninitialized 12, 'org/apache/flink/api/java/typeutils/runtime/kryo/Serializers$SpecificInstanceCollectionSerializerForArrayList' }
 *   Bytecode:
 *     0x0000000: 2b12 05b6 000b bb00 0c59 1205 bb00 0d59
 *     0x0000010: bb00 0659 b700 0eb7 000f b700 10b6 0011
 *     0x0000020: 57b1
 * </pre>
 */
public class AvroKryoClassloadingTest {

	@Test
	public void testKryoInChildClasspath() throws Exception {
		final Class<?> avroClass = AvroKryoSerializerUtils.class;

		final URL avroLocation = avroClass.getProtectionDomain().getCodeSource().getLocation();
		final URL kryoLocation = Kryo.class.getProtectionDomain().getCodeSource().getLocation();

		final ClassLoader parentClassLoader = new FilteredClassLoader(
				avroClass.getClassLoader(), AvroKryoSerializerUtils.class.getName());

		final ClassLoader userAppClassLoader = FlinkUserCodeClassLoaders.childFirst(
				new URL[] { avroLocation, kryoLocation },
				parentClassLoader,
				CoreOptions.ALWAYS_PARENT_FIRST_LOADER.defaultValue().split(";"));

		final Class<?> userLoadedAvroClass = Class.forName(avroClass.getName(), false, userAppClassLoader);
		assertNotEquals(avroClass, userLoadedAvroClass);

		// call the 'addAvroGenericDataArrayRegistration(...)' method
		final Method m = userLoadedAvroClass.getMethod("addAvroGenericDataArrayRegistration", LinkedHashMap.class);

		final LinkedHashMap<String, ?> map = new LinkedHashMap<>();
		m.invoke(userLoadedAvroClass.newInstance(), map);

		assertEquals(1, map.size());
	}
}
