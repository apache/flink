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

package org.apache.flink.table.types.extraction;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.HintFlag;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.extraction.utils.DataTypeHintMock;
import org.apache.flink.table.types.extraction.utils.DataTypeTemplate;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.TypeInformationRawType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.utils.DataTypeFactoryMock;
import org.apache.flink.types.Row;

import org.hamcrest.Matcher;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static org.apache.flink.util.CoreMatchers.containsCause;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link DataTypeExtractor}.
 */
@RunWith(Parameterized.class)
public class DataTypeExtractorTest {

	@Parameters
	public static List<TestSpec> testData() {
		return Arrays.asList(
			// simple extraction of INT
			TestSpec
				.forType(Integer.class)
				.expectDataType(DataTypes.INT()),

			// simple extraction of BYTES
			TestSpec
				.forType(byte[].class)
				.expectDataType(DataTypes.BYTES()),

			// extraction from hint conversion class
			TestSpec
				.forType(
					new DataTypeHintMock() {
						@Override
						public Class<?> bridgedTo() {
							return Long.class;
						}
					},
					Object.class)
				.expectDataType(DataTypes.BIGINT()),

			// missing precision/scale
			TestSpec
				.forType(BigDecimal.class)
				.expectErrorMessage("Values of 'java.math.BigDecimal' need fixed precision and scale."),

			// unsupported Object type exception
			TestSpec
				.forType(Object.class)
				.expectErrorMessage(
					"Cannot extract a data type from a pure 'java.lang.Object' class. " +
						"Usually, this indicates that class information is missing or got lost. " +
						"Please specify a more concrete class or treat it as a RAW type."),

			// unsupported Row type exception
			TestSpec
				.forType(Row.class)
				.expectErrorMessage(
					"Cannot extract a data type from a pure 'org.apache.flink.types.Row' class. " +
						"Please use annotations to define field names and field types."),

			// explicit precision/scale through data type
			TestSpec
				.forType(
					new DataTypeHintMock() {
						@Override
						public String value() {
							return "DECIMAL(5, 3)";
						}
					},
					BigDecimal.class)
				.expectDataType(DataTypes.DECIMAL(5, 3)),

			// default precision/scale
			TestSpec
				.forType(
					new DataTypeHintMock() {
						@Override
						public int defaultDecimalPrecision() {
							return 5;
						}

						@Override
						public int defaultDecimalScale() {
							return 3;
						}
					},
					BigDecimal.class)
				.expectDataType(DataTypes.DECIMAL(5, 3)),

			// different bridging class via default conversion
			TestSpec
				.forType(java.sql.Timestamp.class)
				.expectDataType(DataTypes.TIMESTAMP(9).bridgedTo(java.sql.Timestamp.class)),

			// different bridging class via type itself
			TestSpec
				.forType(
					new DataTypeHintMock() {
						@Override
						public String value() {
							return "TIMESTAMP(0)";
						}
					},
					java.sql.Timestamp.class)
				.expectDataType(DataTypes.TIMESTAMP(0).bridgedTo(java.sql.Timestamp.class)),

			// default seconds for TIMESTAMP
			TestSpec
				.forType(
					new DataTypeHintMock() {
						@Override
						public int defaultSecondPrecision() {
							return 6;
						}
					},
					java.time.LocalDateTime.class)
				.expectDataType(DataTypes.TIMESTAMP(6)),

			// default second precision for INTERVAL
			TestSpec
				.forType(
					new DataTypeHintMock() {
						@Override
						public int defaultSecondPrecision() {
							return 3;
						}
					},
					java.time.Duration.class)
				.expectDataType(DataTypes.INTERVAL(DataTypes.SECOND(3))),

			// default year precision 2 for INTERVAL
			TestSpec
				.forType(
					new DataTypeHintMock() {
						@Override
						public int defaultYearPrecision() {
							return 2;
						}
					},
					java.time.Period.class)
				.expectDataType(DataTypes.INTERVAL(DataTypes.YEAR(2), DataTypes.MONTH())),

			// default year precision 0 for INTERVAL
			TestSpec
				.forType(
					new DataTypeHintMock() {
						@Override
						public int defaultYearPrecision() {
							return 0;
						}
					},
					java.time.Period.class)
				.expectDataType(DataTypes.INTERVAL(DataTypes.MONTH())),

			// RAW with default serializer
			TestSpec
				.forType(
					new DataTypeHintMock() {
						@Override
						public HintFlag allowRawGlobally() {
							return HintFlag.TRUE;
						}
					},
					Object[][].class)
				.lookupExpects(Object.class)
				.expectDataType(
					DataTypes.ARRAY(
						DataTypes.ARRAY(
							DataTypes.RAW(new GenericTypeInfo<>(Object.class))))),

			// RAW with custom serializer
			TestSpec
				.forType(
					new DataTypeHintMock() {
						@Override
						public String value() {
							return "RAW";
						}

						@Override
						public Class<? extends TypeSerializer<?>> rawSerializer() {
							return IntSerializer.class;
						}
					},
					Integer.class)
				.expectDataType(DataTypes.RAW(Integer.class, new IntSerializer())),

			// RAW with different conversion class
			TestSpec
				.forType(
					new DataTypeHintMock() {
						@Override
						public String value() {
							return "RAW";
						}

						@Override
						public Class<?> bridgedTo() {
							return Integer.class;
						}
					},
					Object.class)
				.lookupExpects(Integer.class)
				.expectDataType(DataTypes.RAW(new GenericTypeInfo<>(Integer.class))),

			// MAP type with type variable magic
			TestSpec
				.forGeneric(TableFunction.class, 0, TableFunctionWithMapLevel2.class)
				.expectDataType(DataTypes.MAP(DataTypes.BIGINT(), DataTypes.BOOLEAN())),

			// ARRAY type with type variable magic
			TestSpec
				.forGeneric(TableFunction.class, 0, TableFunctionWithGenericArray1.class)
				.expectDataType(DataTypes.ARRAY(DataTypes.INT())),

			// invalid type variable
			TestSpec
				.forGeneric(TableFunction.class, 0, TableFunctionWithHashMap.class)
				.expectErrorMessage(
					"Could not extract a data type from 'java.util.HashMap<java.lang.Integer, java.lang.String>'. " +
						"Interpreting it as a structured type was also not successful."),

			// simple structured type without RAW type
			TestSpec
				.forType(SimplePojo.class)
				.expectDataType(getSimplePojoDataType(SimplePojo.class)),

			// complex nested structured type annotation on top of type
			TestSpec
				.forType(ComplexPojo.class)
				.lookupExpects(Object.class)
				.expectDataType(getComplexPojoDataType(ComplexPojo.class, SimplePojo.class)),

			// structured type with missing generics
			TestSpec
				.forType(SimplePojoWithGeneric.class)
				.expectErrorMessage(
					"Unresolved type variable 'S'. A data type cannot be extracted from a type variable. " +
						"The original content might have been erased due to Java type erasure."),

			// structured type with annotation on top of field
			TestSpec
				.forGeneric(TableFunction.class, 0, TableFunctionWithGenericPojo.class)
				.lookupExpects(Object.class)
				.expectDataType(getComplexPojoDataType(ComplexPojoWithGeneric.class, SimplePojoWithGeneric.class)),

			// extraction with generic interfaces
			TestSpec
				.forGeneric(BaseInterface.class, 0, ConcreteClass.class)
				.expectDataType(DataTypes.STRING()),

			// structured type with hierarchy
			TestSpec
				.forType(SimplePojoWithGenericHierarchy.class)
				.expectDataType(getSimplePojoDataType(SimplePojoWithGenericHierarchy.class)),

			// structured type with different getter and setter flavors
			TestSpec
				.forType(ComplexPojoWithGettersAndSetters.class)
				.lookupExpects(Object.class)
				.expectDataType(getComplexPojoDataType(ComplexPojoWithGettersAndSetters.class, SimplePojo.class)),

			// structure type with missing setter
			TestSpec
				.forType(SimplePojoWithMissingSetter.class)
				.expectErrorMessage(
					"Field 'stringField' of class '" + SimplePojoWithMissingSetter.class.getName() + "' is " +
						"mutable but is neither publicly accessible nor does it have a corresponding setter method."),

			// structure type with missing getter
			TestSpec
				.forType(SimplePojoWithMissingGetter.class)
				.expectErrorMessage(
					"Field 'stringField' of class '" + SimplePojoWithMissingGetter.class.getName() + "' is " +
						"neither publicly accessible nor does it have a corresponding getter method."),

			// structured type with assigning constructor
			TestSpec
				.forType(SimplePojoWithAssigningConstructor.class)
				.expectDataType(getSimplePojoDataType(SimplePojoWithAssigningConstructor.class)),

			// assigning constructor defines field order
			TestSpec
				.forType(PojoWithCustomFieldOrder.class)
				.expectDataType(getPojoWithCustomOrderDataType(PojoWithCustomFieldOrder.class)),

			// Flink tuples
			TestSpec
				.forGeneric(TableFunction.class, 0, TableFunctionWithTuples.class)
				.expectDataType(getOuterTupleDataType()),

			// many annotations that partially override each other
			TestSpec
				.forType(SimplePojoWithManyAnnotations.class)
				.expectDataType(getSimplePojoDataType(SimplePojoWithManyAnnotations.class)),

			// many annotations that partially override each other
			TestSpec
				.forType(ComplexPojoWithManyAnnotations.class)
				.lookupExpects(Object.class)
				.expectDataType(getComplexPojoDataType(ComplexPojoWithManyAnnotations.class, SimplePojo.class)),

			// method with varargs
			TestSpec
				.forMethodParameter(IntegerVarArg.class, 1)
				.expectDataType(DataTypes.ARRAY(DataTypes.INT().notNull().bridgedTo(int.class))),

			// method with generic parameter
			TestSpec
				.forMethodParameter(IntegerVarArg.class, 0)
				.expectDataType(DataTypes.INT()),

			// method with generic return type
			TestSpec
				.forMethodOutput(IntegerVarArg.class)
				.expectDataType(DataTypes.INT())
		);
	}

	@Parameter
	public TestSpec testSpec;

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testExtraction() {
		if (testSpec.expectedErrorMessage != null) {
			thrown.expect(ValidationException.class);
			thrown.expectCause(errorMatcher(testSpec));
		}
		runExtraction(testSpec);
	}

	// --------------------------------------------------------------------------------------------
	// Test utilities
	// --------------------------------------------------------------------------------------------

	/**
	 * Test specification shared with the Scala tests.
	 */
	static class TestSpec {

		private final DataTypeFactoryMock typeFactory = new DataTypeFactoryMock();

		private final Function<DataTypeFactory, DataType> extractor;

		private @Nullable DataType expectedDataType;

		private @Nullable String expectedErrorMessage;

		private TestSpec(Function<DataTypeFactory, DataType> extractor) {
			this.extractor = extractor;
		}

		static TestSpec forType(Type type) {
			return new TestSpec((lookup) -> DataTypeExtractor.extractFromType(lookup, type));
		}

		static TestSpec forType(DataTypeHint hint, Type type) {
			return new TestSpec((lookup) ->
				DataTypeExtractor.extractFromType(lookup, DataTypeTemplate.fromAnnotation(lookup, hint), type));
		}

		static TestSpec forGeneric(Class<?> baseClass, int genericPos, Type contextType) {
			return new TestSpec((lookup) ->
				DataTypeExtractor.extractFromGeneric(lookup, baseClass, genericPos, contextType));
		}

		static TestSpec forMethodParameter(Class<?> clazz, int paramPos) {
			final Method method = clazz.getMethods()[0];
			return new TestSpec((lookup) ->
				DataTypeExtractor.extractFromMethodParameter(lookup, clazz, method, paramPos));
		}

		static TestSpec forMethodOutput(Class<?> clazz) {
			final Method method = clazz.getMethods()[0];
			return new TestSpec((lookup) ->
				DataTypeExtractor.extractFromMethodOutput(lookup, clazz, method));
		}

		boolean hasErrorMessage() {
			return expectedErrorMessage != null;
		}

		TestSpec lookupExpects(Class<?> lookupClass) {
			typeFactory.dataType = Optional.of(DataTypes.RAW(new GenericTypeInfo<>(lookupClass)));
			typeFactory.expectedClass = Optional.of(lookupClass);
			return this;
		}

		TestSpec expectDataType(DataType expectedDataType) {
			this.expectedDataType = expectedDataType;
			return this;
		}

		TestSpec expectErrorMessage(String expectedErrorMessage) {
			this.expectedErrorMessage = expectedErrorMessage;
			return this;
		}
	}

	static void runExtraction(TestSpec testSpec) {
		final DataType dataType = testSpec.extractor.apply(testSpec.typeFactory);
		if (testSpec.expectedDataType != null) {
			assertThat(dataType, equalTo(testSpec.expectedDataType));
		}
	}

	static Matcher<Throwable> errorMatcher(TestSpec testSpec) {
		return containsCause(new ValidationException(testSpec.expectedErrorMessage));
	}

	/**
	 * Testing data type shared with the Scala tests.
	 */
	static DataType getSimplePojoDataType(Class<?> simplePojoClass) {
		final StructuredType.Builder builder = StructuredType.newBuilder(simplePojoClass);
		builder.attributes(
			Arrays.asList(
				new StructuredType.StructuredAttribute("intField", new IntType(true)),
				new StructuredType.StructuredAttribute("primitiveBooleanField", new BooleanType(false)),
				new StructuredType.StructuredAttribute("primitiveIntField", new IntType(false)),
				new StructuredType.StructuredAttribute("stringField", new VarCharType(VarCharType.MAX_LENGTH))));
		builder.setFinal(true);
		builder.setInstantiable(true);
		final StructuredType structuredType = builder.build();

		final Map<String, DataType> fields = new HashMap<>();
		fields.put("intField", DataTypes.INT());
		fields.put("primitiveBooleanField", DataTypes.BOOLEAN().notNull().bridgedTo(boolean.class));
		fields.put("primitiveIntField", DataTypes.INT().notNull().bridgedTo(int.class));
		fields.put("stringField", DataTypes.STRING());

		return new FieldsDataType(structuredType, simplePojoClass, fields);
	}

	/**
	 * Testing data type shared with the Scala tests.
	 */
	static DataType getComplexPojoDataType(Class<?> complexPojoClass, Class<?> simplePojoClass) {
		final StructuredType.Builder builder = StructuredType.newBuilder(complexPojoClass);
		builder.attributes(
			Arrays.asList(
				new StructuredType.StructuredAttribute(
					"mapField",
					new MapType(new VarCharType(VarCharType.MAX_LENGTH), new IntType())),
				new StructuredType.StructuredAttribute(
					"simplePojoField",
					getSimplePojoDataType(simplePojoClass).getLogicalType()),
				new StructuredType.StructuredAttribute(
					"someObject",
					new TypeInformationRawType<>(new GenericTypeInfo<>(Object.class)))));
		builder.setFinal(true);
		builder.setInstantiable(true);
		final StructuredType structuredType = builder.build();

		final Map<String, DataType> fields = new HashMap<>();
		fields.put("mapField", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()));
		fields.put("simplePojoField", getSimplePojoDataType(simplePojoClass));
		fields.put("someObject", DataTypes.RAW(new GenericTypeInfo<>(Object.class)));

		return new FieldsDataType(structuredType, complexPojoClass, fields);
	}

	/**
	 * Testing data type shared with the Scala tests.
	 */
	public static DataType getPojoWithCustomOrderDataType(Class<?> pojoClass) {
		final StructuredType.Builder builder = StructuredType.newBuilder(pojoClass);
		builder.attributes(
			Arrays.asList(
				new StructuredType.StructuredAttribute(
					"z",
					new BigIntType()),
				new StructuredType.StructuredAttribute(
					"y",
					new BooleanType()),
				new StructuredType.StructuredAttribute(
					"x",
					new IntType())));
		builder.setFinal(true);
		builder.setInstantiable(true);
		final StructuredType structuredType = builder.build();

		final Map<String, DataType> fields = new HashMap<>();
		fields.put("z", DataTypes.BIGINT());
		fields.put("y", DataTypes.BOOLEAN());
		fields.put("x", DataTypes.INT());

		return new FieldsDataType(structuredType, pojoClass, fields);
	}

	private static DataType getOuterTupleDataType() {
		final StructuredType.Builder builder = StructuredType.newBuilder(Tuple2.class);
		builder.attributes(
			Arrays.asList(
				new StructuredType.StructuredAttribute(
					"f0",
					new IntType()),
				new StructuredType.StructuredAttribute(
					"f1",
					getInnerTupleDataType().getLogicalType())));
		builder.setFinal(true);
		builder.setInstantiable(true);
		final StructuredType structuredType = builder.build();

		final Map<String, DataType> fields = new HashMap<>();
		fields.put("f0", DataTypes.INT());
		fields.put("f1", getInnerTupleDataType());

		return new FieldsDataType(structuredType, Tuple2.class, fields);
	}

	private static DataType getInnerTupleDataType() {
		final StructuredType.Builder builder = StructuredType.newBuilder(Tuple2.class);
		builder.attributes(
			Arrays.asList(
				new StructuredType.StructuredAttribute(
					"f0",
					new VarCharType(VarCharType.MAX_LENGTH)),
				new StructuredType.StructuredAttribute(
					"f1",
					new BooleanType())));
		builder.setFinal(true);
		builder.setInstantiable(true);
		final StructuredType structuredType = builder.build();

		final Map<String, DataType> fields = new HashMap<>();
		fields.put("f0", DataTypes.STRING());
		fields.put("f1", DataTypes.BOOLEAN());

		return new FieldsDataType(structuredType, Tuple2.class, fields);
	}

	// --------------------------------------------------------------------------------------------
	// Test classes for extraction
	// --------------------------------------------------------------------------------------------

	private static class TableFunctionWithMapLevel0<K, V> extends TableFunction<Map<K, V>> {

	}

	private static class TableFunctionWithMapLevel1<V> extends TableFunctionWithMapLevel0<Long, V> {

	}

	private static class TableFunctionWithMapLevel2 extends TableFunctionWithMapLevel1<Boolean> {

	}

	// --------------------------------------------------------------------------------------------

	private static class TableFunctionWithGenericArray0<T> extends TableFunction<T[]> {

	}

	private static class TableFunctionWithGenericArray1 extends TableFunctionWithGenericArray0<Integer> {

	}

	// --------------------------------------------------------------------------------------------

	private static class TableFunctionWithHashMap extends TableFunction<HashMap<Integer, String>> {

	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Complex POJO with raw types.
	 */
	@SuppressWarnings("unused")
	@DataTypeHint(allowRawGlobally = HintFlag.TRUE)
	public static class ComplexPojo {
		public Map<String, Integer> mapField;
		public SimplePojo simplePojoField;
		public Object someObject;
	}

	/**
	 * Simple POJO with no RAW types.
	 */
	@SuppressWarnings("unused")
	public static class SimplePojo {
		public Integer intField;
		public boolean primitiveBooleanField;
		public int primitiveIntField;
		public String stringField;
	}

	// --------------------------------------------------------------------------------------------

	private static class TableFunctionWithGenericPojo extends TableFunction<ComplexPojoWithGeneric<String, Integer>> {

	}

	/**
	 * In the end this should be the same data type as {@link ComplexPojo}.
	 */
	@SuppressWarnings("unused")
	public static class ComplexPojoWithGeneric<S, I> {
		public Map<S, I> mapField;
		public SimplePojoWithGeneric<S> simplePojoField;
		@DataTypeHint(allowRawGlobally = HintFlag.TRUE)
		public Object someObject;
	}

	/**
	 * In the end this should be the same data type as {@link SimplePojo}.
	 */
	@SuppressWarnings("unused")
	public static class SimplePojoWithGeneric<S> {
		public Integer intField;
		public boolean primitiveBooleanField;
		public int primitiveIntField;
		public S stringField;
	}

	// --------------------------------------------------------------------------------------------

	private interface BaseInterface<T> {
		// no implementation
	}

	private abstract static class GenericClass<T> implements Serializable, BaseInterface<T> {
		// no implementation
	}

	private static class ConcreteClass extends GenericClass<String> {
		// no implementation
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Hierarchy of structured type.
	 */
	public static class SimplePojoWithGenericHierarchy extends SimplePojoWithGeneric<String> {
		// no implementation
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Private member variables.
	 */
	public static class ComplexPojoWithGettersAndSetters {
		private Map<String, Integer> mapField;
		private SimplePojo simplePojoField;
		private @DataTypeHint("RAW") Object someObject;

		// Java-like
		public Map<String, Integer> getMapField() {
			return mapField;
		}

		public void setMapField(Map<String, Integer> mapField) {
			this.mapField = mapField;
		}

		public SimplePojo getSimplePojoField() {
			return simplePojoField;
		}

		public void setSimplePojoField(SimplePojo simplePojoField) {
			this.simplePojoField = simplePojoField;
		}

		// Scala-like
		public Object someObject() {
			return someObject;
		}

		public void someObject(Object someObject) {
			this.someObject = someObject;
		}
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * {@code setStringField()} is missing.
	 */
	@SuppressWarnings("unused")
	public static class SimplePojoWithMissingSetter {
		public Integer intField;
		public boolean primitiveBooleanField;
		public int primitiveIntField;
		private String stringField;

		public String getStringField() {
			return stringField;
		}
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * {@code getStringField()} is missing.
	 */
	@SuppressWarnings({"FieldCanBeLocal", "unused"})
	public static class SimplePojoWithMissingGetter {
		public Integer intField;
		public boolean primitiveBooleanField;
		public int primitiveIntField;
		private String stringField;

		public void setStringField(String stringField) {
			this.stringField = stringField;
		}
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Immutable type.
	 */
	public static class SimplePojoWithAssigningConstructor {
		public final Integer intField;
		public final boolean primitiveBooleanField;
		public final int primitiveIntField;
		public final String stringField;

		public SimplePojoWithAssigningConstructor(
				Integer intField,
				boolean primitiveBooleanField,
				int primitiveIntField,
				String stringField) {
			this.intField = intField;
			this.primitiveBooleanField = primitiveBooleanField;
			this.primitiveIntField = primitiveIntField;
			this.stringField = stringField;
		}
	}

	// --------------------------------------------------------------------------------------------

	private static class TableFunctionWithTuples extends TableFunction<Tuple2<Integer, Tuple2<String, Boolean>>> {

	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Constructor reorders fields.
	 */
	public static class PojoWithCustomFieldOrder {

		public Integer x;
		public Boolean y;
		public Long z;

		public PojoWithCustomFieldOrder(long z, boolean y, int x) {
			this.z = z;
			this.y = y;
			this.x = x;
		}

		@SuppressWarnings("unused")
		public PojoWithCustomFieldOrder(long z, boolean y, int x, int additional) {
			this(z, y, x);
		}
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * In the end this should be the same data type as {@link SimplePojo}.
	 */
	@SuppressWarnings("unused")
	@DataTypeHint(forceRawPattern = {"java.lang."})
	public static class SimplePojoWithManyAnnotations {
		public @DataTypeHint("INT") Integer intField;
		public @DataTypeHint(bridgedTo = boolean.class) Object primitiveBooleanField;
		public @DataTypeHint(value = "INT NOT NULL", bridgedTo = int.class) Object primitiveIntField;
		@DataTypeHint(forceRawPattern = {})
		public String stringField;
	}

	/**
	 * In the end this should be the same data type as {@link ComplexPojo}.
	 */
	@SuppressWarnings("unused")
	@DataTypeHint(allowRawPattern = {"java.lang"})
	public static class ComplexPojoWithManyAnnotations {
		public @DataTypeHint("MAP<STRING, INT>") Object mapField;
		public SimplePojo simplePojoField;
		public Object someObject;
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Generic Varargs in parameters.
	 */
	public static class VarArgMethod<T> {
		@SuppressWarnings("unused")
		public T eval(T i, int... more) {
			return null;
		}
	}

	/**
	 * Resolvable parameters.
	 */
	public static class IntegerVarArg extends VarArgMethod<Integer> {
		// nothing to do
	}
}
