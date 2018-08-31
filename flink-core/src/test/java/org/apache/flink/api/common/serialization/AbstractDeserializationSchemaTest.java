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

package org.apache.flink.api.common.serialization;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.util.JSONPObject;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests for {@link AbstractDeserializationSchema}.
 */
@SuppressWarnings("serial")
public class AbstractDeserializationSchemaTest {

	@Test
	public void testTypeExtractionTuple() {
		TypeInformation<Tuple2<byte[], byte[]>> type = new TupleSchema().getProducedType();
		TypeInformation<Tuple2<byte[], byte[]>> expected = TypeInformation.of(new TypeHint<Tuple2<byte[], byte[]>>(){});
		assertEquals(expected, type);
	}

	@Test
	public void testTypeExtractionTupleAnonymous() {
		TypeInformation<Tuple2<byte[], byte[]>> type = new AbstractDeserializationSchema<Tuple2<byte[], byte[]>>() {
			@Override
			public Tuple2<byte[], byte[]> deserialize(byte[] message) throws IOException {
				throw new UnsupportedOperationException();
			}
		}.getProducedType();

		TypeInformation<Tuple2<byte[], byte[]>> expected = TypeInformation.of(new TypeHint<Tuple2<byte[], byte[]>>(){});
		assertEquals(expected, type);
	}

	@Test
	public void testTypeExtractionGeneric() {
		TypeInformation<JSONPObject> type = new JsonSchema().getProducedType();
		TypeInformation<JSONPObject> expected = TypeInformation.of(new TypeHint<JSONPObject>(){});
		assertEquals(expected, type);
	}

	@Test
	public void testTypeExtractionGenericAnonymous() {
		TypeInformation<JSONPObject> type = new AbstractDeserializationSchema<JSONPObject>() {
			@Override
			public JSONPObject deserialize(byte[] message) throws IOException {
				throw new UnsupportedOperationException();
			}
		}.getProducedType();

		TypeInformation<JSONPObject> expected = TypeInformation.of(new TypeHint<JSONPObject>(){});
		assertEquals(expected, type);
	}

	@Test
	public void testTypeExtractionRawException() {
		try {
			new RawSchema();
			fail();
		} catch (FlinkRuntimeException e) {
			// expected
		}
	}

	@Test
	public void testTypeExtractionGenericException() {
		try {
			new GenericSchema<>();
			fail();
		} catch (FlinkRuntimeException e) {
			// expected
		}
	}

	@Test
	public void testIndirectGenericExtension() {
		TypeInformation<String> type = new IndirectExtension().getProducedType();
		assertEquals(BasicTypeInfo.STRING_TYPE_INFO, type);
	}

	// ------------------------------------------------------------------------
	//  Test types
	// ------------------------------------------------------------------------

	private static class TupleSchema extends AbstractDeserializationSchema<Tuple2<byte[], byte[]>> {

		@Override
		public Tuple2<byte[], byte[]> deserialize(byte[] message) throws IOException {
			throw new UnsupportedOperationException();
		}
	}

	private static class JsonSchema extends AbstractDeserializationSchema<JSONPObject> {

		@Override
		public JSONPObject deserialize(byte[] message) throws IOException {
			throw new UnsupportedOperationException();
		}
	}

	@SuppressWarnings("rawtypes")
	private static class RawSchema extends AbstractDeserializationSchema {

		@Override
		public Object deserialize(byte[] message) throws IOException {
			throw new UnsupportedOperationException();
		}
	}

	private static class GenericSchema<T> extends AbstractDeserializationSchema<T> {

		@Override
		public T deserialize(byte[] message) throws IOException {
			throw new UnsupportedOperationException();
		}
	}

	private static class IndirectExtension extends GenericSchema<String> {

		@Override
		public String deserialize(byte[] message) throws IOException {
			throw new UnsupportedOperationException();
		}
	}
}
