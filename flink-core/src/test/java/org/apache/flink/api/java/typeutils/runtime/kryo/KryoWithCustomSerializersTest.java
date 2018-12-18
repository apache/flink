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

import java.util.Collection;
import java.util.HashSet;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.runtime.AbstractGenericTypeSerializerTest;
import org.joda.time.LocalDate;
import org.junit.Test;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

@SuppressWarnings("unchecked")
public class KryoWithCustomSerializersTest extends AbstractGenericTypeSerializerTest {
	

	@Test
	public void testJodaTime(){
		Collection<LocalDate> b = new HashSet<LocalDate>();

		b.add(new LocalDate(1L));
		b.add(new LocalDate(2L));

		runTests(b);
	}

	@Override
	protected <T> TypeSerializer<T> createSerializer(Class<T> type) {
		ExecutionConfig conf = new ExecutionConfig();
		conf.registerTypeWithKryoSerializer(LocalDate.class, LocalDateSerializer.class);
		TypeInformation<T> typeInfo = new GenericTypeInfo<T>(type);
		return typeInfo.createSerializer(conf);
	}

	public static final class LocalDateSerializer extends Serializer<LocalDate> implements java.io.Serializable {

		private static final long serialVersionUID = 1L;

		@Override
		public void write(Kryo kryo, Output output, LocalDate object) {
			output.writeInt(object.getYear());
			output.writeInt(object.getMonthOfYear());
			output.writeInt(object.getDayOfMonth());
		}

		@Override
		public LocalDate read(Kryo kryo, Input input, Class<LocalDate> type) {
			return new LocalDate(input.readInt(), input.readInt(), input.readInt());
		}
	}
}
