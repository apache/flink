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

package org.apache.flink.table.annotation;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.logical.LogicalType;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * A hint that influences the reflection-based extraction of a {@link DataType}.
 *
 * <p>Data type hints can parameterize or replace the default extraction logic of individual function parameters
 * and return types, structured classes, or fields of structured classes. An implementer can choose to
 * what extent the default extraction logic should be modified.
 *
 * <p>The following examples show how to explicitly specify data types, how to parameterize the extraction
 * logic, or how to accept any data type as an input data type:
 *
 * <p>{@code @DataTypeHint("INT")} defines an INT data type with a default conversion class.
 *
 * <p>{@code @DataTypeHint(value = "TIMESTAMP(3)", bridgedTo = java.sql.Timestamp.class)} defines a TIMESTAMP
 * data type of millisecond precision with an explicit conversion class.
 *
 * <p>{@code @DataTypeHint(value = "RAW", bridgedTo = MyCustomClass.class)} defines a RAW data type with
 * Flink's default serializer for class {@code MyCustomClass}.
 *
 * <p>{@code @DataTypeHint(value = "RAW", rawSerializer = MyCustomSerializer.class)} defines a RAW data type
 * with a custom serializer class.
 *
 * <p>{@code @DataTypeHint(version = V1, allowRawGlobally = TRUE)} parameterizes the extraction by requesting
 * a extraction logic version of 1 and allowing the RAW data type in this structured type (and possibly
 * nested fields).
 *
 * <p>{@code @DataTypeHint(bridgedTo = MyPojo.class, allowRawGlobally = TRUE)} defines that a type should be
 * extracted from the given conversion class but with parameterized extraction for allowing RAW types.
 *
 * <p>{@code @DataTypeHint(inputGroup = ANY)} defines that the input validation should accept any
 * data type.
 *
 * <p>Note: All hint parameters are optional. Hint parameters defined on top of a structured type are
 * inherited by all (deeply) nested fields unless annotated differently. For example, all occurrences of
 * {@link java.math.BigDecimal} will be extracted as {@code DECIMAL(12, 2)} if the enclosing structured
 * class is annotated with {@code @DataTypeHint(defaultDecimalPrecision = 12, defaultDecimalScale = 2)}. Individual
 * field annotations allow to deviate from those default values.
 *
 * <p>A data type hint on top of a table or aggregate function is similar to defining {@link FunctionHint#output()}
 * for the output type of the function.
 *
 * @see FunctionHint
 */
@PublicEvolving
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.METHOD, ElementType.FIELD, ElementType.PARAMETER})
public @interface DataTypeHint {

	// Note to implementers:
	// Because "null" is not supported as an annotation value. Every annotation parameter *must* have
	// some representation for unknown values in order to merge multi-level annotations.

	// --------------------------------------------------------------------------------------------
	// Explicit data type specification
	// --------------------------------------------------------------------------------------------

	/**
	 * The explicit string representation of a data type. See {@link DataTypes} for a list of supported
	 * data types. For example, {@code INT} for an integer data type or {@code DECIMAL(12, 5)} for decimal
	 * data type with precision 12 and scale 5.
	 *
	 * <p>Use an unparameterized {@code RAW} string for explicitly declaring an opaque data type without
	 * entering a full type string. For Flink's default RAW serializer, use {@code @DataTypeHint("RAW")}
	 * or more specific {@code @DataTypeHint(value = "RAW", bridgedTo = MyCustomClass.class)}. For a custom
	 * RAW serializer, use {@code @DataTypeHint(value = "RAW", rawSerializer = MyCustomSerializer.class)}.
	 *
	 * <p>By default, the empty string represents an undefined data type which means that it will be
	 * derived automatically.
	 *
	 * <p>Use {@link #inputGroup()} for accepting a group of similar data types if this hint is used
	 * to enrich input arguments.
	 *
	 * @see LogicalType#asSerializableString()
	 * @see DataTypes
	 */
	String value() default "";

	/**
	 * Adds a hint that data should be represented using the given class when entering or leaving
	 * the table ecosystem.
	 *
	 * <p>If an explicit data type has been defined via {@link #value()}, a supported conversion class
	 * depends on the logical type and its nullability property.
	 *
	 * <p>If an explicit data type has not been defined via {@link #value()}, this class is used for
	 * reflective extraction of a data type.
	 *
	 * <p>Please see the implementation of {@link LogicalType#supportsInputConversion(Class)},
	 * {@link LogicalType#supportsOutputConversion(Class)}, or the documentation for more information
	 * about supported conversions.
	 *
	 * <p>By default, the conversion class is reflectively extracted.
	 *
	 * @see DataType#bridgedTo(Class)
	 */
	Class<?> bridgedTo() default void.class;

	/**
	 * Adds a hint that defines a custom serializer that should be used for serializing and deserializing
	 * opaque RAW types. It is used if {@link #value()} is explicitly defined as an unparameterized {@code RAW}
	 * string or if (possibly nested) fields in a structured type need to be handled as an opaque type.
	 *
	 * <p>By default, Flink's default RAW serializer is used.
	 *
	 * @see DataTypes#RAW(Class, TypeSerializer)
	 */
	Class<? extends TypeSerializer<?>> rawSerializer() default UnknownSerializer.class;

	// --------------------------------------------------------------------------------------------
	// Group of data types specification
	// --------------------------------------------------------------------------------------------

	/**
	 * This hint parameter influences the extraction of a {@link TypeInference} in functions. It adds
	 * a hint for accepting pre-defined groups of similar types, i.e., more than just one explicit data type.
	 *
	 * <p>Note: This hint parameter is only interpreted when used in function hints or next to arguments
	 * of implementation methods. It has highest precedence above all other hint parameter.
	 *
	 * <p>Some examples:
	 * <pre>
	 * {@code
	 *   // expects an integer for the first input argument and allows any data type for the second
	 *   @FunctionHint(
	 *     input = [@DataTypeHint("INT"), @DataTypeHint(inputGroup = ANY)],
	 *     output = @DataTypeHint("BOOLEAN")
	 *   )
	 *
	 *   // expects an integer for the first input argument and allows any data type for the second
	 *   eval(int i, @DataTypeHint(inputGroup = ANY) Object o)
	 * }
	 * </pre>
	 */
	InputGroup inputGroup() default InputGroup.UNKNOWN;

	// --------------------------------------------------------------------------------------------
	// Parameterization of the reflection-based extraction
	// --------------------------------------------------------------------------------------------

	/**
	 * Version that describes the expected behavior of the reflection-based data type extraction.
	 *
	 * <p>It is meant for future backward compatibility. Whenever the extraction logic is changed,
	 * old function and structured type classes should still return the same data type as before when
	 * versioned accordingly.
	 *
	 * <p>By default, the version is always the most recent one.
	 */
	ExtractionVersion version() default ExtractionVersion.UNKNOWN;

	/**
	 * Defines that a RAW data type may be used for all classes that cannot be mapped to any SQL-like
	 * data type or cause an error.
	 *
	 * <p>By default, this parameter is set to {@code false} which means that an exception is thrown
	 * for unmapped types. This is helpful to identify and fix faulty implementations. It is generally
	 * recommended to use SQL-like types instead of enabling RAW opaque types.
	 *
	 * <p>If RAW types cannot be avoided, they should be enabled only in designated areas (i.e., within
	 * package prefixes using {@link #allowRawPattern()}) in order to not swallow all errors. However,
	 * this parameter globally enables RAW types for the annotated class and all nested fields.
	 *
	 * <p>This parameter has higher precedence than {@link #allowRawPattern()}.
	 *
	 * @see DataTypes#RAW(Class, TypeSerializer)
	 */
	HintFlag allowRawGlobally() default HintFlag.UNKNOWN;

	/**
	 * Defines that a RAW data type may be used for all classes that cannot be mapped to any SQL-like
	 * data type or cause an error if their class name starts with or is equal to one of the given patterns.
	 *
	 * <p>For example, if some Joda time classes cannot be mapped to any SQL-like data type, one can
	 * define the class prefix {@code "org.joda.time"}. Some classes might be handled as structured types
	 * on a best effort basis but others will be RAW data types if necessary.
	 *
	 * <p>By default, the pattern list is empty which means that an exception is thrown for unmapped
	 * types. This is helpful to identify and fix faulty implementations. It is generally recommended
	 * to use SQL-like types instead of enabling RAW opaque types.
	 *
	 * <p>If RAW types cannot be avoided, this parameter should be used to enabled them only in designated
	 * areas (i.e., within package prefixes) in order to not swallow all errors.
	 *
	 * <p>This parameter has lower precedence than {@link #allowRawGlobally()} which would globally allow
	 * RAW types in the annotated class and all nested fields.
	 *
	 * @see DataTypes#RAW(Class, TypeSerializer)
	 */
	String[] allowRawPattern() default {""};

	/**
	 * Defines that a RAW data type must be used for all classes if their class name starts with or
	 * is equal to one of the given patterns.
	 *
	 * <p>For example, one can define the class prefix {@code "org.joda.time", "java.math.BigDecimal"} which
	 * means that all Joda time classes and Java's {@link java.math.BigDecimal} will be handled as RAW data
	 * types regardless if they could be mapped to a more SQL-like data type.
	 *
	 * <p>By default, the pattern list is empty which means that an exception is thrown for unmapped
	 * types. This is helpful to identify and fix faulty implementations. It is generally recommended
	 * to use SQL-like types instead of enabling RAW opaque types.
	 *
	 * <p>If RAW types cannot be avoided, they should be enabled only in designated areas (i.e., within
	 * package prefixes) in order to not swallow all errors. However, compared to {@link #allowRawPattern()},
	 * this parameter forces to skip the extraction entirely for the given prefixes instead of trying
	 * to match a class to a more SQL-like data type.
	 *
	 * <p>This parameter has the highest precedence of all data type related hint parameters.
	 *
	 * @see DataTypes#RAW(Class, TypeSerializer)
	 */
	String[] forceRawPattern() default {""};

	/**
	 * Defines a default precision for all decimal data types that are extracted.
	 *
	 * <p>By default, decimals are not extracted from classes such as {@link java.math.BigDecimal} because
	 * they don't define a fixed precision and scale which is required in the SQL type system.
	 */
	int defaultDecimalPrecision() default -1;

	/**
	 * Defines a default scale for all decimal data types that are extracted.
	 *
	 * <p>By default, decimals are not extracted from classes such as {@link java.math.BigDecimal} because
	 * they don't define a fixed precision and scale which is required in the SQL type system.
	 */
	int defaultDecimalScale() default -1;

	/**
	 * Defines a default year precision for all year-month intervals that are extracted. If set to {@code 0},
	 * an {@code INTERVAL MONTH} data type is extracted.
	 *
	 * <p>By default, {@code INTERVAL YEAR(4) TO MONTH} data types are extracted from classes such as
	 * {@link java.time.Period}.
	 */
	int defaultYearPrecision() default -1;

	/**
	 * Defines a default fractional second precision for all day-time intervals and timestamps that
	 * are extracted.
	 *
	 * <p>By default, those data types are extracted with nano second precision.
	 */
	int defaultSecondPrecision() default -1;
}
