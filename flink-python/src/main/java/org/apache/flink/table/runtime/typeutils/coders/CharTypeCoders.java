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

package org.apache.flink.table.runtime.typeutils.coders;

import org.apache.flink.table.dataformat.BinaryString;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.StringUtf8Coder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * CharTypeCoders include all kinds of coders for CharType/VarCharType.
 */
public class CharTypeCoders extends AbstractCoderFinder {

	private static final StringUtf8Coder STRING_UTF_8_CODER = StringUtf8Coder.of();

	private static final BinaryStringCoder BINARY_STRING_CODER = BinaryStringCoder.of();

	public static CharTypeCoders of() {
		return INSTANCE;
	}

	private static final CharTypeCoders INSTANCE = new CharTypeCoders();

	private Map<Class<?>, Coder<?>> coders = new HashMap<>();

	private Map<Class<?>, Class<?>> externalToInternal = new HashMap<>();

	private CharTypeCoders() {
		coders.put(String.class, STRING_UTF_8_CODER);
		coders.put(BinaryString.class, BINARY_STRING_CODER);
		externalToInternal.put(String.class, BinaryString.class);
	}

	@Override
	Map<Class<?>, Coder<?>> getCoders() {
		return coders;
	}

	@Override
	Map<Class<?>, Class<?>> externalToInterval() {
		return externalToInternal;
	}

	@Override
	String getDataTypeName() {
		return "CharType/VarCharType";
	}

	/**
	 * A {@link Coder} for {@link BinaryString}.
	 */
	public static class BinaryStringCoder extends Coder<BinaryString> {
		private static final long serialVersionUID = 1L;

		public static BinaryStringCoder of() {
			return INSTANCE;
		}

		private static final BinaryStringCoder INSTANCE = new BinaryStringCoder();

		private BinaryStringCoder() {
		}

		@Override
		public void encode(BinaryString value, OutputStream outStream) throws IOException {
			if (value == null) {
				throw new CoderException("Cannot encode a null BinaryString for BinaryStringCoder");
			}
			STRING_UTF_8_CODER.encode(value.toString(), outStream);
		}

		@Override
		public BinaryString decode(InputStream inStream) throws IOException {
			return BinaryString.fromString(STRING_UTF_8_CODER.decode(inStream));
		}

		@Override
		public List<? extends Coder<?>> getCoderArguments() {
			return Collections.emptyList();
		}

		@Override
		public void verifyDeterministic() {
		}
	}
}
