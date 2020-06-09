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

package org.apache.flink.api.common.typeinfo;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.EitherTypeInfo;
import org.apache.flink.api.java.typeutils.EnumTypeInfo;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.PojoField;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.typeutils.ValueTypeInfo;
import org.apache.flink.types.Either;
import org.apache.flink.types.Row;
import org.apache.flink.types.Value;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class gives access to the type information of the most common types for which Flink
 * has built-in serializers and comparators.
 *
 * <p>In many cases, Flink tries to analyze generic signatures of functions to determine return
 * types automatically. This class is intended for cases where type information has to be
 * supplied manually or cases where automatic type inference results in an inefficient type.
 *
 * <p>Please note that the Scala API and Table API have dedicated Types classes.
 * (See <code>org.apache.flink.api.scala.Types</code> and <code>org.apache.flink.table.api.Types</code>)
 *
 * <p>A more convenient alternative might be a {@link TypeHint}.
 *
 * @see TypeInformation#of(Class) specify type information based on a class that will be analyzed
 * @see TypeInformation#of(TypeHint) specify type information based on a {@link TypeHint}
 */
@PublicEvolving
public class Types {

	/**
	 * Returns type information for {@link java.lang.Void}. Does not support a null value.
	 */
	public static final TypeInformation<Void> VOID = BasicTypeInfo.VOID_TYPE_INFO;

	/**
	 * Returns type information for {@link java.lang.String}. Supports a null value.
	 */
	public static final TypeInformation<String> STRING = BasicTypeInfo.STRING_TYPE_INFO;

	/**
	 * Returns type information for both a primitive <code>byte</code> and {@link java.lang.Byte}.
	 * Does not support a null value.
	 */
	public static final TypeInformation<Byte> BYTE = BasicTypeInfo.BYTE_TYPE_INFO;

	/**
	 * Returns type information for both a primitive <code>boolean</code> and {@link java.lang.Boolean}.
	 * Does not support a null value.
	 */
	public static final TypeInformation<Boolean> BOOLEAN = BasicTypeInfo.BOOLEAN_TYPE_INFO;

	/**
	 * Returns type information for both a primitive <code>short</code> and {@link java.lang.Short}.
	 * Does not support a null value.
	 */
	public static final TypeInformation<Short> SHORT = BasicTypeInfo.SHORT_TYPE_INFO;

	/**
	 * Returns type information for both a primitive <code>int</code> and {@link java.lang.Integer}.
	 * Does not support a null value.
	 */
	public static final TypeInformation<Integer> INT = BasicTypeInfo.INT_TYPE_INFO;

	/**
	 * Returns type information for both a primitive <code>long</code> and {@link java.lang.Long}.
	 * Does not support a null value.
	 */
	public static final TypeInformation<Long> LONG = BasicTypeInfo.LONG_TYPE_INFO;

	/**
	 * Returns type information for both a primitive <code>float</code> and {@link java.lang.Float}.
	 * Does not support a null value.
	 */
	public static final TypeInformation<Float> FLOAT = BasicTypeInfo.FLOAT_TYPE_INFO;

	/**
	 * Returns type information for both a primitive <code>double</code> and {@link java.lang.Double}.
	 * Does not support a null value.
	 */
	public static final TypeInformation<Double> DOUBLE = BasicTypeInfo.DOUBLE_TYPE_INFO;

	/**
	 * Returns type information for both a primitive <code>char</code> and {@link java.lang.Character}.
	 * Does not support a null value.
	 */
	public static final TypeInformation<Character> CHAR = BasicTypeInfo.CHAR_TYPE_INFO;

	/**
	 * Returns type information for {@link java.math.BigDecimal}. Supports a null value.
	 */
	public static final TypeInformation<BigDecimal> BIG_DEC = BasicTypeInfo.BIG_DEC_TYPE_INFO;

	/**
	 * Returns type information for {@link java.math.BigInteger}. Supports a null value.
	 */
	public static final TypeInformation<BigInteger> BIG_INT = BasicTypeInfo.BIG_INT_TYPE_INFO;

	/**
	 * Returns type information for {@link java.sql.Date}. Supports a null value.
	 */
	public static final TypeInformation<Date> SQL_DATE = SqlTimeTypeInfo.DATE;

	/**
	 * Returns type information for {@link java.sql.Time}. Supports a null value.
	 */
	public static final TypeInformation<Time> SQL_TIME = SqlTimeTypeInfo.TIME;

	/**
	 * Returns type information for {@link java.sql.Timestamp}. Supports a null value.
	 */
	public static final TypeInformation<Timestamp> SQL_TIMESTAMP = SqlTimeTypeInfo.TIMESTAMP;

	/**
	 * Returns type information for {@link java.time.LocalDate}. Supports a null value.
	 */
	public static final TypeInformation<LocalDate> LOCAL_DATE = LocalTimeTypeInfo.LOCAL_DATE;

	/**
	 * Returns type information for {@link java.time.LocalTime}. Supports a null value.
	 */
	public static final TypeInformation<LocalTime> LOCAL_TIME = LocalTimeTypeInfo.LOCAL_TIME;

	/**
	 * Returns type information for {@link java.time.LocalDateTime}. Supports a null value.
	 */
	public static final TypeInformation<LocalDateTime> LOCAL_DATE_TIME = LocalTimeTypeInfo.LOCAL_DATE_TIME;

	/**
	 * Returns type infomation for {@link java.time.Instant}. Supports a null value.
	 */
	public static final TypeInformation<Instant> INSTANT = BasicTypeInfo.INSTANT_TYPE_INFO;

	//CHECKSTYLE.OFF: MethodName

	/**
	 * Returns type information for {@link org.apache.flink.types.Row} with fields of the given types.
	 * A row itself must not be null.
	 *
	 * <p>A row is a fixed-length, null-aware composite type for storing multiple values in a
	 * deterministic field order. Every field can be null regardless of the field's type.
	 * The type of row fields cannot be automatically inferred; therefore, it is required to provide
	 * type information whenever a row is produced.
	 *
	 * <p>The schema of rows can have up to <code>Integer.MAX_VALUE</code> fields, however, all row instances
	 * must strictly adhere to the schema defined by the type info.
	 *
	 * <p>This method generates type information with fields of the given types; the fields have
	 * the default names (f0, f1, f2 ..).
	 *
	 * @param types The types of the row fields, e.g., Types.STRING, Types.INT
	 */
	public static TypeInformation<Row> ROW(TypeInformation<?>... types) {
		return new RowTypeInfo(types);
	}

	/**
	 * Returns type information for {@link org.apache.flink.types.Row} with fields of the given types and
	 * with given names. A row must not be null.
	 *
	 * <p>A row is a fixed-length, null-aware composite type for storing multiple values in a
	 * deterministic field order. Every field can be null independent of the field's type.
	 * The type of row fields cannot be automatically inferred; therefore, it is required to provide
	 * type information whenever a row is used.
	 *
	 * <p>The schema of rows can have up to <code>Integer.MAX_VALUE</code> fields, however, all row instances
	 * must strictly adhere to the schema defined by the type info.
	 *
	 * <p>Example use: {@code ROW_NAMED(new String[]{"name", "number"}, Types.STRING, Types.INT)}.
	 *
	 * @param fieldNames array of field names
	 * @param types array of field types
	 */
	public static TypeInformation<Row> ROW_NAMED(String[] fieldNames, TypeInformation<?>... types) {
		return new RowTypeInfo(types, fieldNames);
	}

	/**
	 * Returns type information for subclasses of Flink's {@link org.apache.flink.api.java.tuple.Tuple}
	 * (namely {@link org.apache.flink.api.java.tuple.Tuple0} till {@link org.apache.flink.api.java.tuple.Tuple25})
	 * with fields of the given types. A tuple must not be null.
	 *
	 * <p>A tuple is a fixed-length composite type for storing multiple values in a
	 * deterministic field order. Fields of a tuple are typed. Tuples are the most efficient composite
	 * type; a tuple does not support null-valued fields unless the type of the field supports nullability.
	 *
	 * @param types The types of the tuple fields, e.g., Types.STRING, Types.INT
	 */
	public static <T extends Tuple> TypeInformation<T> TUPLE(TypeInformation<?>... types) {
		return new TupleTypeInfo<>(types);
	}

	/**
	 * Returns type information for typed subclasses of Flink's {@link org.apache.flink.api.java.tuple.Tuple}.
	 * Typed subclassed are classes that extend {@link org.apache.flink.api.java.tuple.Tuple0} till
	 * {@link org.apache.flink.api.java.tuple.Tuple25} to provide types for all fields and might add
	 * additional getters and setters for better readability. Additional member fields must not be added.
	 * A tuple must not be null.
	 *
	 * <p>A tuple is a fixed-length composite type for storing multiple values in a
	 * deterministic field order. Fields of a tuple are typed. Tuples are the most efficient composite
	 * type; a tuple does not support null-valued fields unless the type of the field supports nullability.
	 *
	 * <p>The generic types for all fields of the tuple can be defined in a hierarchy of subclasses.
	 *
	 * <p>If Flink's type analyzer is unable to extract a tuple type information with
	 * type information for all fields, an {@link org.apache.flink.api.common.functions.InvalidTypesException}
	 * is thrown.
	 *
	 * <p>Example use:
	 * <pre>
	 * {@code
	 *   class MyTuple extends Tuple2<Integer, String> {
	 *
	 *     public int getId() { return f0; }
	 *
	 *     public String getName() { return f1; }
	 *   }
	 * }
	 *
	 * Types.TUPLE(MyTuple.class)
	 * </pre>
	 *
	 * @param tupleSubclass A subclass of {@link org.apache.flink.api.java.tuple.Tuple0} till
	 *                      {@link org.apache.flink.api.java.tuple.Tuple25} that defines all field types and
	 *                      does not add any additional fields
	 */
	public static <T extends Tuple> TypeInformation<T> TUPLE(Class<T> tupleSubclass) {
		final TypeInformation<T> ti = TypeExtractor.createTypeInfo(tupleSubclass);
		if (ti instanceof TupleTypeInfo) {
			return ti;
		}
		throw new InvalidTypesException("Tuple type expected but was: " + ti);
	}

	/**
	 * Returns type information for a POJO (Plain Old Java Object).
	 *
	 * <p>A POJO class is public and standalone (no non-static inner class). It has a public no-argument
	 * constructor. All non-static, non-transient fields in the class (and all superclasses) are either public
	 * (and non-final) or have a public getter and a setter method that follows the Java beans naming
	 * conventions for getters and setters.
	 *
	 * <p>A POJO is a fixed-length and null-aware composite type. Every field can be null independent
	 * of the field's type.
	 *
	 * <p>The generic types for all fields of the POJO can be defined in a hierarchy of subclasses.
	 *
	 * <p>If Flink's type analyzer is unable to extract a valid POJO type information with
	 * type information for all fields, an {@link org.apache.flink.api.common.functions.InvalidTypesException}
	 * is thrown. Alternatively, you can use {@link Types#POJO(Class, Map)} to specify all fields manually.
	 *
	 * @param pojoClass POJO class to be analyzed by Flink
	 */
	public static <T> TypeInformation<T> POJO(Class<T> pojoClass) {
		final TypeInformation<T> ti = TypeExtractor.createTypeInfo(pojoClass);
		if (ti instanceof PojoTypeInfo) {
			return ti;
		}
		throw new InvalidTypesException("POJO type expected but was: " + ti);
	}

	/**
	 * Returns type information for a POJO (Plain Old Java Object) and allows to specify all fields manually.
	 *
	 * <p>A POJO class is public and standalone (no non-static inner class). It has a public no-argument
	 * constructor. All non-static, non-transient fields in the class (and all superclasses) are either public
	 * (and non-final) or have a public getter and a setter method that follows the Java beans naming
	 * conventions for getters and setters.
	 *
	 * <p>A POJO is a fixed-length, null-aware composite type with non-deterministic field order. Every field
	 * can be null independent of the field's type.
	 *
	 * <p>The generic types for all fields of the POJO can be defined in a hierarchy of subclasses.
	 *
	 * <p>If Flink's type analyzer is unable to extract a POJO field, an
	 * {@link org.apache.flink.api.common.functions.InvalidTypesException} is thrown.
	 *
	 * <p><strong>Note:</strong> In most cases the type information of fields can be determined automatically,
	 * we recommend to use {@link Types#POJO(Class)}.
	 *
	 * @param pojoClass POJO class
	 * @param fields map of fields that map a name to type information. The map key is the name of
	 *               the field and the value is its type.
	 */
	public static <T> TypeInformation<T> POJO(Class<T> pojoClass, Map<String, TypeInformation<?>> fields) {
		final List<PojoField> pojoFields = new ArrayList<>(fields.size());
		for (Map.Entry<String, TypeInformation<?>> field : fields.entrySet()) {
			final Field f = TypeExtractor.getDeclaredField(pojoClass, field.getKey());
			if (f == null) {
				throw new InvalidTypesException("Field '" + field.getKey() + "'could not be accessed.");
			}
			pojoFields.add(new PojoField(f, field.getValue()));
		}

		return new PojoTypeInfo<>(pojoClass, pojoFields);
	}

	/**
	 * Returns generic type information for any Java object. The serialization logic will
	 * use the general purpose serializer Kryo.
	 *
	 * <p>Generic types are black-boxes for Flink, but allow any object and null values in fields.
	 *
	 * <p>By default, serialization of this type is not very efficient. Please read the documentation
	 * about how to improve efficiency (namely by pre-registering classes).
	 *
	 * @param genericClass any Java class
	 */
	public static <T> TypeInformation<T> GENERIC(Class<T> genericClass) {
		return new GenericTypeInfo<>(genericClass);
	}

	/**
	 * Returns type information for Java arrays of primitive type (such as <code>byte[]</code>). The array
	 * must not be null.
	 *
	 * @param elementType element type of the array (e.g. Types.BOOLEAN, Types.INT, Types.DOUBLE)
	 */
	public static TypeInformation<?> PRIMITIVE_ARRAY(TypeInformation<?> elementType) {
		if (elementType == BOOLEAN) {
			return PrimitiveArrayTypeInfo.BOOLEAN_PRIMITIVE_ARRAY_TYPE_INFO;
		} else if (elementType == BYTE) {
			return PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO;
		} else if (elementType == SHORT) {
			return PrimitiveArrayTypeInfo.SHORT_PRIMITIVE_ARRAY_TYPE_INFO;
		} else if (elementType == INT) {
			return PrimitiveArrayTypeInfo.INT_PRIMITIVE_ARRAY_TYPE_INFO;
		} else if (elementType == LONG) {
			return PrimitiveArrayTypeInfo.LONG_PRIMITIVE_ARRAY_TYPE_INFO;
		} else if (elementType == FLOAT) {
			return PrimitiveArrayTypeInfo.FLOAT_PRIMITIVE_ARRAY_TYPE_INFO;
		} else if (elementType == DOUBLE) {
			return PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO;
		} else if (elementType == CHAR) {
			return PrimitiveArrayTypeInfo.CHAR_PRIMITIVE_ARRAY_TYPE_INFO;
		}
		throw new IllegalArgumentException("Invalid element type for a primitive array.");
	}

	/**
	 * Returns type information for Java arrays of object types (such as <code>String[]</code>,
	 * <code>Integer[]</code>). The array itself must not be null. Null values for elements are supported.
	 *
	 * @param elementType element type of the array
	 */
	@SuppressWarnings("unchecked")
	public static <E> TypeInformation<E[]> OBJECT_ARRAY(TypeInformation<E> elementType) {
		if (elementType == Types.STRING) {
			return (TypeInformation) BasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO;
		}
		return ObjectArrayTypeInfo.getInfoFor(elementType);
	}

	/**
	 * Returns type information for Flink value types (classes that implement
	 * {@link org.apache.flink.types.Value}). Built-in value types do not support null values (except
	 * for {@link org.apache.flink.types.StringValue}).
	 *
	 * <p>Value types describe their serialization and deserialization manually. Instead of going
	 * through a general purpose serialization framework. A value type is reasonable when general purpose
	 * serialization would be highly inefficient. The wrapped value can be altered, allowing programmers to
	 * reuse objects and take pressure off the garbage collector.
	 *
	 * <p>Flink provides built-in value types for all Java primitive types (such as
	 * {@link org.apache.flink.types.BooleanValue}, {@link org.apache.flink.types.IntValue}) as well
	 * as {@link org.apache.flink.types.StringValue}, {@link org.apache.flink.types.NullValue},
	 * {@link org.apache.flink.types.ListValue}, and {@link org.apache.flink.types.MapValue}.
	 *
	 * @param valueType class that implements {@link org.apache.flink.types.Value}
	 */
	public static <V extends Value> TypeInformation<V> VALUE(Class<V> valueType) {
		return new ValueTypeInfo<>(valueType);
	}

	/**
	 * Returns type information for a Java {@link java.util.Map}. A map must not be null. Null values
	 * in keys are not supported. An entry's value can be null.
	 *
	 * <p>By default, maps are untyped and treated as a generic type in Flink; therefore, it is useful
	 * to pass type information whenever a map is used.
	 *
	 * <p><strong>Note:</strong> Flink does not preserve the concrete {@link Map} type. It converts a map into {@link HashMap} when
	 * copying or deserializing.
	 *
	 * @param keyType type information for the map's keys
	 * @param valueType type information for the map's values
	 */
	public static <K, V> TypeInformation<Map<K, V>> MAP(TypeInformation<K> keyType, TypeInformation<V> valueType) {
		return new MapTypeInfo<>(keyType, valueType);
	}

	/**
	 * Returns type information for a Java {@link java.util.List}. A list must not be null. Null values
	 * in elements are not supported.
	 *
	 * <p>By default, lists are untyped and treated as a generic type in Flink; therefore, it is useful
	 * to pass type information whenever a list is used.
	 *
	 * <p><strong>Note:</strong> Flink does not preserve the concrete {@link List} type. It converts a list into {@link ArrayList} when
	 * copying or deserializing.
	 *
	 * @param elementType type information for the list's elements
	 */
	public static <E> TypeInformation<List<E>> LIST(TypeInformation<E> elementType) {
		return new ListTypeInfo<>(elementType);
	}

	/**
	 * Returns type information for Java enumerations. Null values are not supported.
	 *
	 * @param enumType enumeration class extending {@link java.lang.Enum}
	 */
	public static <E extends Enum<E>> TypeInformation<E> ENUM(Class<E> enumType) {
		return new EnumTypeInfo<>(enumType);
	}

	/**
	 * Returns type information for Flink's {@link org.apache.flink.types.Either} type. Null values
	 * are not supported.
	 *
	 * <p>Either type can be used for a value of two possible types.
	 *
	 * <p>Example use: <code>Types.EITHER(Types.VOID, Types.INT)</code>
	 *
	 * @param leftType type information of left side / {@link org.apache.flink.types.Either.Left}
	 * @param rightType type information of right side / {@link org.apache.flink.types.Either.Right}
	 */
	public static <L, R> TypeInformation<Either<L, R>> EITHER(TypeInformation<L> leftType, TypeInformation<R> rightType) {
		return new EitherTypeInfo<>(leftType, rightType);
	}

	//CHECKSTYLE.ON: MethodName
}
