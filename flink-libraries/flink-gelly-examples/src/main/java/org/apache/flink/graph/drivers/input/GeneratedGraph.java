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

package org.apache.flink.graph.drivers.input;

import org.apache.commons.lang3.text.WordUtils;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.client.program.ProgramParametrizationException;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.asm.translate.TranslateFunction;
import org.apache.flink.graph.asm.translate.TranslateGraphIds;
import org.apache.flink.graph.asm.translate.translators.LongValueToStringValue;
import org.apache.flink.graph.asm.translate.translators.LongValueToUnsignedIntValue;
import org.apache.flink.graph.drivers.parameter.ChoiceParameter;
import org.apache.flink.graph.drivers.parameter.ParameterizedBase;
import org.apache.flink.types.ByteValue;
import org.apache.flink.types.CharValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;
import org.apache.flink.types.ShortValue;

/**
 * Base class for generated graphs.
 *
 * @param <K> graph ID type
 */
public abstract class GeneratedGraph<K>
extends ParameterizedBase
implements Input<K, NullValue, NullValue> {

	private static final String BYTE = "byte";
	private static final String NATIVE_BYTE = "nativeByte";

	private static final String SHORT = "short";
	private static final String NATIVE_SHORT = "nativeShort";

	private static final String CHAR = "char";
	private static final String NATIVE_CHAR = "nativeChar";

	private static final String INTEGER = "integer";
	private static final String NATIVE_INTEGER = "nativeInteger";

	private static final String LONG = "long";
	private static final String NATIVE_LONG = "nativeLong";

	private static final String STRING = "string";
	private static final String NATIVE_STRING = "nativeString";

	private ChoiceParameter type = new ChoiceParameter(this, "type")
		.setDefaultValue(INTEGER)
		.addChoices(LONG, STRING)
		.addHiddenChoices(BYTE, NATIVE_BYTE, SHORT, NATIVE_SHORT, CHAR, NATIVE_CHAR, NATIVE_INTEGER, NATIVE_LONG, NATIVE_STRING);

	/**
	 * The vertex count is verified to be no greater than the capacity of the
	 * selected data type. All vertices must be counted even if skipped or
	 * unused when generating graph edges.
	 *
	 * @return number of vertices configured for the graph
	 */
	protected abstract long vertexCount();

	/**
	 * Generate the graph as configured.
	 *
	 * @param env Flink execution environment
	 * @return generated graph
	 * @throws Exception on error
	 */
	protected abstract Graph<K, NullValue, NullValue> generate(ExecutionEnvironment env) throws Exception;

	/**
	 * Get the name of the type.
	 *
	 * @return name of the type
	 */
	protected String getTypeName() {
		return WordUtils.capitalize(type.getValue());
	}

	@Override
	public Graph<K, NullValue, NullValue> create(ExecutionEnvironment env)
			throws Exception {
		long maxVertexCount = Long.MAX_VALUE;
		TranslateFunction translator = null;

		switch (type.getValue()) {
			case BYTE:
				maxVertexCount = LongValueToUnsignedByteValue.MAX_VERTEX_COUNT;
				translator = new LongValueToUnsignedByteValue();
				break;

			case NATIVE_BYTE:
				maxVertexCount = LongValueToUnsignedByte.MAX_VERTEX_COUNT;
				translator = new LongValueToUnsignedByte();
				break;

			case SHORT:
				maxVertexCount = LongValueToUnsignedShortValue.MAX_VERTEX_COUNT;
				translator = new LongValueToUnsignedShortValue();
				break;

			case NATIVE_SHORT:
				maxVertexCount = LongValueToUnsignedShort.MAX_VERTEX_COUNT;
				translator = new LongValueToUnsignedShort();
				break;

			case CHAR:
				maxVertexCount = LongValueToCharValue.MAX_VERTEX_COUNT;
				translator = new LongValueToCharValue();
				break;

			case NATIVE_CHAR:
				maxVertexCount = LongValueToChar.MAX_VERTEX_COUNT;
				translator = new LongValueToChar();
				break;

			case INTEGER:
				maxVertexCount = LongValueToUnsignedIntValue.MAX_VERTEX_COUNT;
				translator = new LongValueToUnsignedIntValue();
				break;

			case NATIVE_INTEGER:
				maxVertexCount = LongValueToUnsignedInt.MAX_VERTEX_COUNT;
				translator = new LongValueToUnsignedInt();
				break;

			case LONG:
				break;

			case NATIVE_LONG:
				translator = new LongValueToLong();
				break;

			case STRING:
				translator = new LongValueToStringValue();
				break;

			case NATIVE_STRING:
				translator = new LongValueToString();
				break;

			default:
				throw new ProgramParametrizationException("Unknown type '" + type.getValue() + "'");
		}

		long vertexCount = vertexCount();
		if (vertexCount > maxVertexCount) {
			throw new ProgramParametrizationException("Vertex count '" + vertexCount +
				"' must be no greater than " + maxVertexCount +  " for type '" + type.getValue() + "'.");
		}

		Graph<K, NullValue, NullValue> graph = generate(env);

		if (translator != null) {
			graph = (Graph<K, NullValue, NullValue>) graph.run(new TranslateGraphIds(translator));
		}

		return graph;
	}

	/**
	 * Translate {@link LongValue} to {@link ByteValue}.
	 *
	 * Throws {@link RuntimeException} for byte overflow.
	 */
	static class LongValueToUnsignedByteValue
	implements TranslateFunction<LongValue, ByteValue> {
		public static final long MAX_VERTEX_COUNT = 1L << 8;

		@Override
		public ByteValue translate(LongValue value, ByteValue reuse)
				throws Exception {
			if (reuse == null) {
				reuse = new ByteValue();
			}

			long l = value.getValue();

			if (l < 0 || l >= MAX_VERTEX_COUNT) {
				throw new IllegalArgumentException("Cannot cast long value " + value + " to byte.");
			} else {
				reuse.setValue((byte) (l & (MAX_VERTEX_COUNT - 1)));
			}

			return reuse;
		}
	}

	/**
	 * Translate {@link LongValue} to {@link Byte}.
	 *
	 * Throws {@link RuntimeException} for byte overflow.
	 */
	static class LongValueToUnsignedByte
	implements TranslateFunction<LongValue, Byte> {
		public static final long MAX_VERTEX_COUNT = 1L << 8;

		@Override
		public Byte translate(LongValue value, Byte reuse)
				throws Exception {
			long l = value.getValue();

			if (l < 0 || l >= MAX_VERTEX_COUNT) {
				throw new IllegalArgumentException("Cannot cast long value " + value + " to byte.");
			}

			return (byte) (l & (MAX_VERTEX_COUNT - 1));
		}
	}

	/**
	 * Translate {@link LongValue} to {@link ShortValue}.
	 *
	 * Throws {@link RuntimeException} for short overflow.
	 */
	static class LongValueToUnsignedShortValue
	implements TranslateFunction<LongValue, ShortValue> {
		public static final long MAX_VERTEX_COUNT = 1L << 16;

		@Override
		public ShortValue translate(LongValue value, ShortValue reuse)
				throws Exception {
			if (reuse == null) {
				reuse = new ShortValue();
			}

			long l = value.getValue();

			if (l < 0 || l >= MAX_VERTEX_COUNT) {
				throw new IllegalArgumentException("Cannot cast long value " + value + " to short.");
			} else {
				reuse.setValue((short) (l & (MAX_VERTEX_COUNT - 1)));
			}

			return reuse;
		}
	}

	/**
	 * Translate {@link LongValue} to {@link Short}.
	 *
	 * Throws {@link RuntimeException} for short overflow.
	 */
	static class LongValueToUnsignedShort
	implements TranslateFunction<LongValue, Short> {
		public static final long MAX_VERTEX_COUNT = 1L << 16;

		@Override
		public Short translate(LongValue value, Short reuse)
				throws Exception {
			long l = value.getValue();

			if (l < 0 || l >= MAX_VERTEX_COUNT) {
				throw new IllegalArgumentException("Cannot cast long value " + value + " to short.");
			}

			return (short) (l & (MAX_VERTEX_COUNT - 1));
		}
	}

	/**
	 * Translate {@link LongValue} to {@link CharValue}.
	 *
	 * Throws {@link RuntimeException} for char overflow.
	 */
	static class LongValueToCharValue
	implements TranslateFunction<LongValue, CharValue> {
		public static final long MAX_VERTEX_COUNT = 1L << 16;

		@Override
		public CharValue translate(LongValue value, CharValue reuse)
				throws Exception {
			if (reuse == null) {
				reuse = new CharValue();
			}

			long l = value.getValue();

			if (l < 0 || l >= MAX_VERTEX_COUNT) {
				throw new IllegalArgumentException("Cannot cast long value " + value + " to char.");
			} else {
				reuse.setValue((char) (l & (MAX_VERTEX_COUNT - 1)));
			}

			return reuse;
		}
	}

	/**
	 * Translate {@link LongValue} to {@code Character}.
	 *
	 * Throws {@link RuntimeException} for char overflow.
	 */
	static class LongValueToChar
	implements TranslateFunction<LongValue, Character> {
		public static final long MAX_VERTEX_COUNT = 1L << 16;

		@Override
		public Character translate(LongValue value, Character reuse)
				throws Exception {
			long l = value.getValue();

			if (l < 0 || l >= MAX_VERTEX_COUNT) {
				throw new IllegalArgumentException("Cannot cast long value " + value + " to char.");
			}

			return (char) (l & (MAX_VERTEX_COUNT - 1));
		}
	}

	/**
	 * Translate {@link LongValue} to {@link Integer}.
	 *
	 * Throws {@link RuntimeException} for integer overflow.
	 */
	static class LongValueToUnsignedInt
	implements TranslateFunction<LongValue, Integer> {
		public static final long MAX_VERTEX_COUNT = 1L << 32;

		@Override
		public Integer translate(LongValue value, Integer reuse)
				throws Exception {
			long l = value.getValue();

			if (l < 0 || l >= MAX_VERTEX_COUNT) {
				throw new IllegalArgumentException("Cannot cast long value " + value + " to integer.");
			}

			return (int) (l & (MAX_VERTEX_COUNT - 1));
		}
	}

	/**
	 * Translate {@link LongValue} to {@link Long}.
	 */
	static class LongValueToLong
	implements TranslateFunction<LongValue, Long> {
		@Override
		public Long translate(LongValue value, Long reuse)
				throws Exception {
			return value.getValue();
		}
	}

	/**
	 * Translate {@link LongValue} to {@link String}.
	 */
	static class LongValueToString
	implements TranslateFunction<LongValue, String> {
		@Override
		public String translate(LongValue value, String reuse)
				throws Exception {
			return Long.toString(value.getValue());
		}
	}
}
