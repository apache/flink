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

package org.apache.flink.api.java.io.csv.custom.type.complex;

import org.apache.flink.types.parser.FieldParser;
import org.apache.flink.types.parser.StringParser;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

/**
 * A JSON parser for {@link GenericsAwareCustomJsonType} type, that is based on StringParser.
 * @param <T> a generic type of the {@link GenericsAwareCustomJsonType} instance.
 */
public class GenericsAwareCustomJsonTypeStringParser<T> extends FieldParser<GenericsAwareCustomJsonType<T>> {

	private final StringParser stringParser = new StringParser();

	{
		stringParser.enableQuotedStringParsing((byte) '\'');
	}

	private TypeReference<GenericsAwareCustomJsonType<T>> typeReference;
	private GenericsAwareCustomJsonType<T> lastResult;

	public GenericsAwareCustomJsonTypeStringParser(TypeReference<GenericsAwareCustomJsonType<T>> typeReference) {
		this.typeReference = typeReference;
	}

	@Override
	protected int parseField(byte[] bytes, int startPos, int limit, byte[] delim, GenericsAwareCustomJsonType<T> reuse) {
		int offset = stringParser.parseField(bytes, startPos, limit, delim, null);
		String stringRepresentation = stringParser.getLastResult();

		ObjectMapper mapper = new ObjectMapper();
		try {
			lastResult = mapper.readValue(stringRepresentation, typeReference);
			return offset;
		} catch (IOException e) {
			e.printStackTrace();
			return -1;
		}
	}

	@Override
	public GenericsAwareCustomJsonType<T> getLastResult() {
		return lastResult;
	}

	@Override
	public GenericsAwareCustomJsonType<T> createValue() {
		return new GenericsAwareCustomJsonType();
	}
}
