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

package org.apache.flink.graph.drivers.transform;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.client.program.ProgramParametrizationException;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.asm.result.TranslatableResult;
import org.apache.flink.graph.asm.translate.TranslateFunction;
import org.apache.flink.graph.asm.translate.TranslateGraphIds;
import org.apache.flink.graph.asm.translate.translators.LongValueToStringValue;
import org.apache.flink.graph.asm.translate.translators.LongValueToUnsignedIntValue;
import org.apache.flink.graph.drivers.parameter.BooleanParameter;
import org.apache.flink.graph.drivers.parameter.ChoiceParameter;
import org.apache.flink.graph.drivers.parameter.ParameterizedBase;
import org.apache.flink.types.ByteValue;
import org.apache.flink.types.CharValue;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.FloatValue;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.ShortValue;
import org.apache.flink.types.StringValue;
import org.apache.flink.util.Collector;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/**
 * Transform the ID type of graphs from {@link LongValue} to byte, short,
 * char, integer, float, double, or string. Both boxed Java types and Flink
 * {@link org.apache.flink.types.Value} types are supported. The result
 * is transformed when necessary to standardize {@code hashCode}.
 */
public class GraphKeyTypeTransform<VV, EV>
extends ParameterizedBase
implements Transform<Graph<LongValue, VV, EV>, Graph<?, VV, EV>, DataSet<TranslatableResult<?>>, DataSet<TranslatableResult<?>>> {

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

	private static final String FLOAT = "float";
	private static final String NATIVE_FLOAT = "nativeFloat";

	private static final String DOUBLE = "double";
	private static final String NATIVE_DOUBLE = "nativeDouble";

	private static final String STRING = "string";
	private static final String NATIVE_STRING = "nativeString";

	private ChoiceParameter type = new ChoiceParameter(this, "type")
		.setDefaultValue(INTEGER)
		.addChoices(LONG, STRING)
		.addHiddenChoices(BYTE, NATIVE_BYTE, SHORT, NATIVE_SHORT, CHAR, NATIVE_CHAR, NATIVE_INTEGER, NATIVE_LONG,
			FLOAT, NATIVE_FLOAT, DOUBLE, NATIVE_DOUBLE, NATIVE_STRING);

	private final BooleanParameter disableTypeReversal = new BooleanParameter(this, "__disable_type_reversal");

	private final long vertexCount;

	/**
	 * Create a key type transform. The type is configured by the command-line
	 * arguments but because many types reduce the size of the vertex ID space
	 * the maximum extent must be set explicitly by the input in order to
	 * perform bounds checking. An n-bit type can store up to 2^n - 1 vertex
	 * identifiers.
	 *
	 * @param vertexCount the size of the vertex set of the input graph
	 */
	public GraphKeyTypeTransform(long vertexCount) {
		this.vertexCount = vertexCount;
	}

	@Override
	public String getIdentity() {
		return type.toString();
	}

	@Override
	public Graph<?, VV, EV> transformInput(Graph<LongValue, VV, EV> input)
			throws Exception {
		// Long.MAX_VALUE is much larger than the number of atoms in the Earth
		// and considered sufficient though representing 63 instead of 64 bits
		long maxVertexCount = Long.MAX_VALUE;
		TranslateFunction<LongValue, ?> translator = null;

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

			case FLOAT:
				maxVertexCount = LongValueToUnsignedFloatValue.MAX_VERTEX_COUNT;
				translator = new LongValueToUnsignedFloatValue();
				break;

			case NATIVE_FLOAT:
				maxVertexCount = LongValueToUnsignedFloat.MAX_VERTEX_COUNT;
				translator = new LongValueToUnsignedFloat();
				break;

			case DOUBLE:
				translator = new LongValueToDoubleValue();
				break;

			case NATIVE_DOUBLE:
				translator = new LongValueToDouble();
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

		if (vertexCount > maxVertexCount) {
			throw new ProgramParametrizationException("Vertex count '" + vertexCount +
				"' must be no greater than " + maxVertexCount +  " for type '" + type.getValue() + "'.");
		}

		if (translator == null) {
			return input;
		} else {
			return (Graph<?, VV, EV>) input.run(new TranslateGraphIds(translator));
		}
	}

	@Override
	public DataSet<TranslatableResult<?>> transformResult(final DataSet<TranslatableResult<?>> result) throws Exception {
		if (disableTypeReversal.getValue()) {
			return result;
		}

		// the minimum vertex count is used for signed types for which results
		// must be translated to a wider type when overflowing to negative
		// values in order to preserve consistent checksums
		long minVertexCount = 0;
		TranslateFunction<?, LongValueWithProperHashCode> translator = null;

		switch (type.getValue()) {
			case BYTE:
				minVertexCount = UnsignedByteValueToLongValueWithProperHashCode.MIN_VERTEX_COUNT;
				translator = new UnsignedByteValueToLongValueWithProperHashCode();
				break;

			case NATIVE_BYTE:
				minVertexCount = UnsignedByteToLongValueWithProperHashCode.MIN_VERTEX_COUNT;
				translator = new UnsignedByteToLongValueWithProperHashCode();
				break;

			case SHORT:
				minVertexCount = UnsignedShortValueToLongValueWithProperHashCode.MIN_VERTEX_COUNT;
				translator = new UnsignedShortValueToLongValueWithProperHashCode();
				break;

			case NATIVE_SHORT:
				minVertexCount = UnsignedShortToLongValueWithProperHashCode.MIN_VERTEX_COUNT;
				translator = new UnsignedShortToLongValueWithProperHashCode();
				break;

			case CHAR:
				break;

			case NATIVE_CHAR:
				break;

			case INTEGER:
				minVertexCount = UnsignedIntValueToLongValueWithProperHashCode.MIN_VERTEX_COUNT;
				translator = new UnsignedIntValueToLongValueWithProperHashCode();
				break;

			case NATIVE_INTEGER:
				minVertexCount = UnsignedIntToLongValueWithProperHashCode.MIN_VERTEX_COUNT;
				translator = new UnsignedIntToLongValueWithProperHashCode();
				break;

			case LONG:
				translator = new LongValueToLongValueWithProperHashCode();
				break;

			case NATIVE_LONG:
				break;

			case FLOAT:
				translator = new UnsignedFloatValueToLongValueWithProperHashCode();
				break;

			case NATIVE_FLOAT:
				translator = new UnsignedFloatToLongValueWithProperHashCode();
				break;

			case DOUBLE:
				translator = new DoubleValueToLongValueWithProperHashCode();
				break;

			case NATIVE_DOUBLE:
				translator = new DoubleToLongValueWithProperHashCode();
				break;

			case STRING:
				translator = new StringValueToLongValueWithProperHashCode();
				break;

			case NATIVE_STRING:
				translator = new StringToLongValueWithProperHashCode();
				break;

			default:
				throw new ProgramParametrizationException("Unknown type '" + type.getValue() + "'");
		}

		if (translator == null || vertexCount < minVertexCount) {
			return result;
		}

		TypeInformation<?> typeInfo = TypeExtractor.createTypeInfo(new ParameterizedType() {
			@Override
			public Type[] getActualTypeArguments() {
				return new Type[]{LongValueWithProperHashCode.class};
			}

			@Override
			public Type getRawType() {
				return result.getType().getTypeClass();
			}

			@Override
			public Type getOwnerType() {
				return null;
			}
		});

		return result
			.flatMap(new TranslateResultIDs(translator))
				.returns(typeInfo)
				.name("Translate results IDs");
	}

	/**
	 * Translate {@link LongValue} to an unsigned {@link ByteValue}.
	 *
	 * <p>Throws {@link RuntimeException} for byte overflow.
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
			}

			reuse.setValue((byte) (l & (MAX_VERTEX_COUNT - 1)));
			return reuse;
		}
	}

	/**
	 * Translate {@link LongValue} to an unsigned {@link Byte}.
	 *
	 * <p>Throws {@link RuntimeException} for byte overflow.
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
	 * Translate {@link LongValue} to an unsigned {@link ShortValue}.
	 *
	 * <p>Throws {@link RuntimeException} for short overflow.
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
			}

			reuse.setValue((short) (l & (MAX_VERTEX_COUNT - 1)));
			return reuse;
		}
	}

	/**
	 * Translate {@link LongValue} to an unsigned {@link Short}.
	 *
	 * <p>Throws {@link RuntimeException} for short overflow.
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
	 * <p>Throws {@link RuntimeException} for char overflow.
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
			}

			reuse.setValue((char) (l & (MAX_VERTEX_COUNT - 1)));
			return reuse;
		}
	}

	/**
	 * Translate {@link LongValue} to {@code Character}.
	 *
	 * <p>Throws {@link RuntimeException} for char overflow.
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
	 * Translate {@link LongValue} to an unsigned {@link Integer}.
	 *
	 * <p>Throws {@link RuntimeException} for integer overflow.
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
	 * Translate {@link LongValue} to an unsigned {@link FloatValue} bit string.
	 */
	static class LongValueToUnsignedFloatValue
	implements TranslateFunction<LongValue, FloatValue> {
		public static final long MAX_VERTEX_COUNT = 1L << 32;

		@Override
		public FloatValue translate(LongValue value, FloatValue reuse)
				throws Exception {
			if (reuse == null) {
				reuse = new FloatValue();
			}

			long l = value.getValue();

			if (l < 0 || l >= MAX_VERTEX_COUNT) {
				throw new IllegalArgumentException("Cannot cast long value " + value + " to float.");
			}

			reuse.setValue(Float.intBitsToFloat((int) (l & (MAX_VERTEX_COUNT - 1))));
			return reuse;
		}
	}

	/**
	 * Translate {@link LongValue} to an unsigned {@link Float} bit string.
	 */
	static class LongValueToUnsignedFloat
	implements TranslateFunction<LongValue, Float> {
		public static final long MAX_VERTEX_COUNT = 1L << 32;

		@Override
		public Float translate(LongValue value, Float reuse)
				throws Exception {
			long l = value.getValue();

			if (l < 0 || l >= MAX_VERTEX_COUNT) {
				throw new IllegalArgumentException("Cannot cast long value " + value + " to float.");
			}

			return Float.intBitsToFloat((int) (l & (MAX_VERTEX_COUNT - 1)));
		}
	}

	/**
	 * Translate {@link LongValue} to a {@link DoubleValue} bit string.
	 */
	static class LongValueToDoubleValue
	implements TranslateFunction<LongValue, DoubleValue> {
		@Override
		public DoubleValue translate(LongValue value, DoubleValue reuse)
				throws Exception {
			if (reuse == null) {
				reuse = new DoubleValue();
			}

			reuse.setValue(Double.longBitsToDouble(value.getValue()));
			return reuse;
		}
	}

	/**
	 * Translate {@link LongValue} to a {@link Double} bit string.
	 */
	static class LongValueToDouble
	implements TranslateFunction<LongValue, Double> {
		@Override
		public Double translate(LongValue value, Double reuse)
				throws Exception {
			return Double.longBitsToDouble(value.getValue());
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

	/**
	 * Translate a {@link TranslatableResult}.
	 *
	 * @param <T> ID type to translate from
	 * @param <U> ID type to translate to
	 */
	private static class TranslateResultIDs<T, U>
	implements FlatMapFunction<TranslatableResult<T>, TranslatableResult<U>> {
		private final TranslateFunction<T, U> translator;

		private transient TranslatableResult<U> reuse = null;

		public TranslateResultIDs(TranslateFunction<T, U> translator) {
			this.translator = translator;
		}

		@Override
		public void flatMap(TranslatableResult<T> value, Collector<TranslatableResult<U>> out)
				throws Exception {
			reuse = value.translate(translator, reuse, out);
		}
	}

	/**
	 * Translate an unsigned {@link ByteValue} to {@link LongValueWithProperHashCode}.
	 */
	static class UnsignedByteValueToLongValueWithProperHashCode
	implements TranslateFunction<ByteValue, LongValueWithProperHashCode> {
		public static final long MIN_VERTEX_COUNT = Byte.MAX_VALUE + 2;

		@Override
		public LongValueWithProperHashCode translate(ByteValue value, LongValueWithProperHashCode reuse)
				throws Exception {
			if (reuse == null) {
				reuse = new LongValueWithProperHashCode();
			}

			reuse.setValue(value.getValue() & 0xff);
			return reuse;
		}
	}

	/**
	 * Translate an unsigned {@link Byte} to {@link LongValueWithProperHashCode}.
	 */
	static class UnsignedByteToLongValueWithProperHashCode
	implements TranslateFunction<Byte, LongValueWithProperHashCode> {
		public static final long MIN_VERTEX_COUNT = Byte.MAX_VALUE + 2;

		@Override
		public LongValueWithProperHashCode translate(Byte value, LongValueWithProperHashCode reuse)
				throws Exception {
			if (reuse == null) {
				reuse = new LongValueWithProperHashCode();
			}

			reuse.setValue(value & 0xff);
			return reuse;
		}
	}

	/**
	 * Translate an unsigned {@link ShortValue} to {@link LongValueWithProperHashCode}.
	 */
	static class UnsignedShortValueToLongValueWithProperHashCode
	implements TranslateFunction<ShortValue, LongValueWithProperHashCode> {
		public static final long MIN_VERTEX_COUNT = Short.MAX_VALUE + 2;

		@Override
		public LongValueWithProperHashCode translate(ShortValue value, LongValueWithProperHashCode reuse)
				throws Exception {
			if (reuse == null) {
				reuse = new LongValueWithProperHashCode();
			}

			reuse.setValue(value.getValue() & 0xffff);
			return reuse;
		}
	}

	/**
	 * Translate an unsigned {@link Short} to {@link LongValueWithProperHashCode}.
	 */
	static class UnsignedShortToLongValueWithProperHashCode
	implements TranslateFunction<Short, LongValueWithProperHashCode> {
		public static final long MIN_VERTEX_COUNT = Short.MAX_VALUE + 2;

		@Override
		public LongValueWithProperHashCode translate(Short value, LongValueWithProperHashCode reuse)
				throws Exception {
			if (reuse == null) {
				reuse = new LongValueWithProperHashCode();
			}

			reuse.setValue(value & 0xffff);
			return reuse;
		}
	}

	/**
	 * Translate an unsigned {@link IntValue} to {@link LongValueWithProperHashCode}.
	 */
	static class UnsignedIntValueToLongValueWithProperHashCode
	implements TranslateFunction<IntValue, LongValueWithProperHashCode> {
		public static final long MIN_VERTEX_COUNT = Integer.MAX_VALUE + 2L;

		@Override
		public LongValueWithProperHashCode translate(IntValue value, LongValueWithProperHashCode reuse)
				throws Exception {
			if (reuse == null) {
				reuse = new LongValueWithProperHashCode();
			}

			reuse.setValue(0xffffffffL & (long) value.getValue());
			return reuse;
		}
	}

	/**
	 * Translate an unsigned {@link Integer} to {@link LongValueWithProperHashCode}.
	 */
	static class UnsignedIntToLongValueWithProperHashCode
	implements TranslateFunction<Integer, LongValueWithProperHashCode> {
		public static final long MIN_VERTEX_COUNT = Integer.MAX_VALUE + 2L;

		@Override
		public LongValueWithProperHashCode translate(Integer value, LongValueWithProperHashCode reuse)
				throws Exception {
			if (reuse == null) {
				reuse = new LongValueWithProperHashCode();
			}

			reuse.setValue(0xffffffffL & (long) value);
			return reuse;
		}
	}

	/**
	 * Translate {@link LongValue} to {@link LongValueWithProperHashCode}.
	 */
	static class LongValueToLongValueWithProperHashCode
	implements TranslateFunction<LongValue, LongValueWithProperHashCode> {
		@Override
		public LongValueWithProperHashCode translate(LongValue value, LongValueWithProperHashCode reuse)
				throws Exception {
			if (reuse == null) {
				reuse = new LongValueWithProperHashCode();
			}

			reuse.setValue(value.getValue());
			return reuse;
		}
	}

	/**
	 * Translate an unsigned {@link FloatValue} bit string to {@link LongValueWithProperHashCode}.
	 */
	static class UnsignedFloatValueToLongValueWithProperHashCode
	implements TranslateFunction<FloatValue, LongValueWithProperHashCode> {
		@Override
		public LongValueWithProperHashCode translate(FloatValue value, LongValueWithProperHashCode reuse)
				throws Exception {
			if (reuse == null) {
				reuse = new LongValueWithProperHashCode();
			}

			reuse.setValue(0xffffffffL & Float.floatToRawIntBits(value.getValue()));
			return reuse;
		}
	}

	/**
	 * Translate an unsigned {@link Float} bit string to {@link LongValueWithProperHashCode}.
	 */
	static class UnsignedFloatToLongValueWithProperHashCode
	implements TranslateFunction<Float, LongValueWithProperHashCode> {
		@Override
		public LongValueWithProperHashCode translate(Float value, LongValueWithProperHashCode reuse)
				throws Exception {
			if (reuse == null) {
				reuse = new LongValueWithProperHashCode();
			}

			reuse.setValue(0xffffffffL & Float.floatToRawIntBits(value));
			return reuse;
		}
	}

	/**
	 * Translate a {@link DoubleValue} bit string to {@link LongValueWithProperHashCode}.
	 */
	static class DoubleValueToLongValueWithProperHashCode
	implements TranslateFunction<DoubleValue, LongValueWithProperHashCode> {
		@Override
		public LongValueWithProperHashCode translate(DoubleValue value, LongValueWithProperHashCode reuse)
				throws Exception {
			if (reuse == null) {
				reuse = new LongValueWithProperHashCode();
			}

			reuse.setValue(Double.doubleToRawLongBits(value.getValue()));
			return reuse;
		}
	}

	/**
	 * Translate a {@link Double} bit string to {@link LongValueWithProperHashCode}.
	 */
	static class DoubleToLongValueWithProperHashCode
	implements TranslateFunction<Double, LongValueWithProperHashCode> {
		@Override
		public LongValueWithProperHashCode translate(Double value, LongValueWithProperHashCode reuse)
				throws Exception {
			if (reuse == null) {
				reuse = new LongValueWithProperHashCode();
			}

			reuse.setValue(Double.doubleToRawLongBits(value));
			return reuse;
		}
	}

	/**
	 * Translate {@link StringValue} to {@link LongValueWithProperHashCode}.
	 */
	static class StringValueToLongValueWithProperHashCode
	implements TranslateFunction<StringValue, LongValueWithProperHashCode> {
		@Override
		public LongValueWithProperHashCode translate(StringValue value, LongValueWithProperHashCode reuse)
				throws Exception {
			if (reuse == null) {
				reuse = new LongValueWithProperHashCode();
			}

			reuse.setValue(Long.parseLong(value.getValue()));
			return reuse;
		}
	}

	/**
	 * Translate {@link String} to {@link LongValueWithProperHashCode}.
	 */
	static class StringToLongValueWithProperHashCode
	implements TranslateFunction<String, LongValueWithProperHashCode> {
		@Override
		public LongValueWithProperHashCode translate(String value, LongValueWithProperHashCode reuse)
				throws Exception {
			if (reuse == null) {
				reuse = new LongValueWithProperHashCode();
			}

			reuse.setValue(Long.valueOf(value));
			return reuse;
		}
	}
}
