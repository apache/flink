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

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeLookup;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.extraction.utils.DataTypeTemplate;
import org.apache.flink.table.types.extraction.utils.ExtractionUtils;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RawType;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.StructuredType.StructuredAttribute;
import org.apache.flink.table.types.utils.ClassDataTypeConverter;

import javax.annotation.Nullable;

import java.lang.reflect.Field;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.table.types.extraction.utils.ExtractionUtils.collectStructuredFields;
import static org.apache.flink.table.types.extraction.utils.ExtractionUtils.collectTypeHierarchy;
import static org.apache.flink.table.types.extraction.utils.ExtractionUtils.createRawType;
import static org.apache.flink.table.types.extraction.utils.ExtractionUtils.extractAssigningConstructor;
import static org.apache.flink.table.types.extraction.utils.ExtractionUtils.extractionError;
import static org.apache.flink.table.types.extraction.utils.ExtractionUtils.isStructuredFieldMutable;
import static org.apache.flink.table.types.extraction.utils.ExtractionUtils.resolveVariable;
import static org.apache.flink.table.types.extraction.utils.ExtractionUtils.toClass;
import static org.apache.flink.table.types.extraction.utils.ExtractionUtils.validateStructuredClass;
import static org.apache.flink.table.types.extraction.utils.ExtractionUtils.validateStructuredFieldReadability;

/**
 * Reflection-based utility that analyzes a given {@link java.lang.reflect.Type}, method, or class to
 * extract a (possibly nested) {@link DataType} from it.
 */
@Internal
public final class DataTypeExtractor {

	private final DataTypeLookup lookup;

	private final String contextExplanation;

	private DataTypeExtractor(DataTypeLookup lookup, String contextExplanation) {
		this.lookup = lookup;
		this.contextExplanation = contextExplanation;
	}

	/**
	 * Extracts a data type from a type without considering surrounding classes or templates.
	 */
	public static DataType extractFromType(
			DataTypeLookup lookup,
			Type type) {
		return extractDataTypeWithClassContext(
			lookup,
			DataTypeTemplate.fromDefaults(),
			null,
			type,
			"");
	}

	/**
	 * Extracts a data type from a type without considering surrounding classes but templates.
	 */
	public static DataType extractFromType(
			DataTypeLookup lookup,
			DataTypeTemplate template,
			Type type) {
		return extractDataTypeWithClassContext(
			lookup,
			template,
			null,
			type,
			"");
	}

	/**
	 * Extracts a data type from a type variable at {@code genericPos} of {@code baseClass} using
	 * the information of the most specific type {@code contextType}.
	 */
	public static DataType extractFromGeneric(
			DataTypeLookup lookup,
			Class<?> baseClass,
			int genericPos,
			Type contextType) {
		final TypeVariable<?> variable = baseClass.getTypeParameters()[genericPos];
		return extractDataTypeWithClassContext(
			lookup,
			DataTypeTemplate.fromDefaults(),
			contextType,
			variable,
			String.format(
				" in generic class '%s' in %s",
				baseClass.getName(),
				contextType.toString()));
	}

	/**
	 * Extracts a data type from a method parameter by considering surrounding classes and parameter
	 * annotation.
	 */
	public static DataType extractFromMethodParameter(
			DataTypeLookup lookup,
			Class<?> baseClass,
			Method method,
			int paramPos) {
		final Parameter parameter = method.getParameters()[paramPos];
		final DataTypeHint hint = parameter.getAnnotation(DataTypeHint.class);
		final DataTypeTemplate template;
		if (hint != null) {
			template = DataTypeTemplate.fromAnnotation(lookup, hint);
		} else {
			template = DataTypeTemplate.fromDefaults();
		}
		return extractDataTypeWithClassContext(
			lookup,
			template,
			baseClass,
			parameter.getParameterizedType(),
			String.format(
				" in parameter %d of method '%s' in class '%s'",
				paramPos,
				method.getName(),
				baseClass.getName()));
	}

	/**
	 * Extracts a data type from a method return type by considering surrounding classes and method
	 * annotation.
	 */
	public static DataType extractFromMethodOutput(
			DataTypeLookup lookup,
			Class<?> baseClass,
			Method method) {
		final DataTypeHint hint = method.getAnnotation(DataTypeHint.class);
		final DataTypeTemplate template;
		if (hint != null) {
			template = DataTypeTemplate.fromAnnotation(lookup, hint);
		} else {
			template = DataTypeTemplate.fromDefaults();
		}
		return extractDataTypeWithClassContext(
			lookup,
			template,
			baseClass,
			method.getGenericReturnType(),
			String.format(
				" in return type of method '%s' in class '%s'",
				method.getName(),
				baseClass.getName()));
	}

	private static DataType extractDataTypeWithClassContext(
			DataTypeLookup lookup,
			DataTypeTemplate outerTemplate,
			@Nullable Type contextType,
			Type type,
			String contextExplanation) {
		final DataTypeExtractor extractor = new DataTypeExtractor(lookup, contextExplanation);
		final List<Type> typeHierarchy;
		if (contextType != null) {
			typeHierarchy = collectTypeHierarchy(contextType);
		} else {
			typeHierarchy = Collections.emptyList();
		}
		return extractor.extractDataTypeOrRaw(outerTemplate, typeHierarchy, type);
	}

	// --------------------------------------------------------------------------------------------

	private DataType extractDataTypeOrRaw(
			DataTypeTemplate outerTemplate,
			List<Type> typeHierarchy,
			Type type) {
		// best effort resolution of type variables, the resolved type can still be a variable
		final Type resolvedType;
		if (type instanceof TypeVariable) {
			resolvedType = resolveVariable(typeHierarchy, (TypeVariable) type);
		} else {
			resolvedType = type;
		}
		// merge outer template with template of type itself
		DataTypeTemplate template = outerTemplate;
		final Class<?> clazz = toClass(resolvedType);
		if (clazz != null) {
			final DataTypeHint hint = clazz.getAnnotation(DataTypeHint.class);
			if (hint != null) {
				template = outerTemplate.mergeWithInnerAnnotation(lookup, hint);
			}
		}
		// main work
		final DataType dataType = extractDataTypeOrRawWithTemplate(template, typeHierarchy, resolvedType);
		// final work
		return closestBridging(dataType, clazz);
	}

	private DataType extractDataTypeOrRawWithTemplate(
			DataTypeTemplate template,
			List<Type> typeHierarchy,
			Type type) {
		// template defines a data type
		if (template.dataType != null) {
			return template.dataType;
		}
		try {
			return extractDataTypeOrError(template, typeHierarchy, type);
		} catch (Throwable t) {
			// ignore the exception and just treat it as RAW type
			final Class<?> clazz = toClass(type);
			if (template.isAllowRawGlobally() || template.isAllowAnyPattern(clazz)) {
				return createRawType(lookup, template.rawSerializer, clazz);
			}
			// forward the root cause otherwise
			throw extractionError(
				t,
				"Could not extract a data type from '%s'%s. " +
					"Please pass the required data type manually or allow RAW types.",
				type.toString(),
				contextExplanation);
		}
	}

	private DataType extractDataTypeOrError(DataTypeTemplate template, List<Type> typeHierarchy, Type type) {
		// still a type variable
		if (type instanceof TypeVariable) {
			throw extractionError(
				"Unresolved type variable '%s'. A data type cannot be extracted from a type variable. " +
					"The original content might have been erased due to Java type erasure.",
				type.toString());
		}

		// ARRAY
		DataType resultDataType = extractArrayType(template, typeHierarchy, type);
		if (resultDataType != null) {
			return resultDataType;
		}

		// skip extraction for enforced patterns early but after arrays
		resultDataType = extractEnforcedRawType(template, type);
		if (resultDataType != null) {
			return resultDataType;
		}

		// PREDEFINED
		resultDataType = extractPredefinedType(template, type);
		if (resultDataType != null) {
			return resultDataType;
		}

		// MAP
		resultDataType = extractMapType(template, typeHierarchy, type);
		if (resultDataType != null) {
			return resultDataType;
		}

		// try interpret the type as a STRUCTURED type
		try {
			return extractStructuredType(template, typeHierarchy, type);
		} catch (Throwable t) {
			throw extractionError(
				t,
				"Could not extract a data type from '%s'. " +
					"Interpreting it as a structured type was also not successful.",
				type.toString());
		}
	}

	private @Nullable DataType extractArrayType(
			DataTypeTemplate template,
			List<Type> typeHierarchy,
			Type type) {
		// for T[]
		if (type instanceof GenericArrayType) {
			final GenericArrayType genericArray = (GenericArrayType) type;
			return DataTypes.ARRAY(
				extractDataTypeOrRaw(template, typeHierarchy, genericArray.getGenericComponentType()));
		}
		// for my.custom.Pojo[][]
		else if (type instanceof Class) {
			final Class<?> clazz = (Class<?>) type;
			if (clazz.isArray()) {
				return DataTypes.ARRAY(
					extractDataTypeOrRaw(template, typeHierarchy, clazz.getComponentType()));
			}
		}
		return null;
	}

	private @Nullable DataType extractEnforcedRawType(DataTypeTemplate template, Type type) {
		final Class<?> clazz = toClass(type);
		if (template.isForceAnyPattern(clazz)) {
			return createRawType(lookup, template.rawSerializer, clazz);
		}
		return null;
	}

	private @Nullable DataType extractPredefinedType(DataTypeTemplate template, Type type) {
		final Class<?> clazz = toClass(type);
		// all predefined types are representable as classes
		if (clazz == null) {
			return null;
		}

		// DECIMAL
		if (clazz == BigDecimal.class) {
			if (template.defaultDecimalPrecision != null && template.defaultDecimalScale != null) {
				return DataTypes.DECIMAL(template.defaultDecimalPrecision, template.defaultDecimalScale);
			} else if (template.defaultDecimalPrecision != null) {
				return DataTypes.DECIMAL(template.defaultDecimalPrecision, 0);
			}
			throw extractionError("Values of '%s' need fixed precision and scale.", BigDecimal.class.getName());
		}

		// TIME
		else if (clazz == java.sql.Time.class || clazz == java.time.LocalTime.class) {
			if (template.defaultSecondPrecision != null) {
				return DataTypes.TIME(template.defaultSecondPrecision)
					.bridgedTo(clazz);
			}
		}

		// TIMESTAMP
		else if (clazz == java.sql.Timestamp.class || clazz == java.time.LocalDateTime.class) {
			if (template.defaultSecondPrecision != null) {
				return DataTypes.TIMESTAMP(template.defaultSecondPrecision)
					.bridgedTo(clazz);
			}
		}

		// TIMESTAMP WITH TIME ZONE
		else if (clazz == java.time.OffsetDateTime.class) {
			if (template.defaultSecondPrecision != null) {
				return DataTypes.TIMESTAMP_WITH_TIME_ZONE(template.defaultSecondPrecision);
			}
		}

		// TIMESTAMP WITH LOCAL TIME ZONE
		else if (clazz == java.time.Instant.class) {
			if (template.defaultSecondPrecision != null) {
				return DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(template.defaultSecondPrecision);
			}
		}

		// INTERVAL SECOND
		else if (clazz == java.time.Duration.class) {
			if (template.defaultSecondPrecision != null) {
				return DataTypes.INTERVAL(DataTypes.SECOND(template.defaultSecondPrecision));
			}
		}

		// INTERVAL YEAR TO MONTH
		else if (clazz == java.time.Period.class) {
			if (template.defaultYearPrecision != null && template.defaultYearPrecision == 0) {
				return DataTypes.INTERVAL(DataTypes.MONTH());
			} else if (template.defaultYearPrecision != null) {
				return DataTypes.INTERVAL(DataTypes.YEAR(template.defaultYearPrecision), DataTypes.MONTH());
			}
		}

		return ClassDataTypeConverter.extractDataType(clazz).orElse(null);
	}

	private @Nullable DataType extractMapType(DataTypeTemplate template, List<Type> typeHierarchy, Type type) {
		final Class<?> clazz = toClass(type);
		if (clazz != Map.class) {
			return null;
		}
		if (!(type instanceof ParameterizedType)) {
			throw extractionError("Raw map type needs generic parameters.");
		}
		final ParameterizedType parameterizedType = (ParameterizedType) type;
		final DataType key = extractDataTypeOrRaw(
			template,
			typeHierarchy,
			parameterizedType.getActualTypeArguments()[0]);
		final DataType value = extractDataTypeOrRaw(
			template,
			typeHierarchy,
			parameterizedType.getActualTypeArguments()[1]);
		return DataTypes.MAP(key, value);
	}

	private DataType extractStructuredType(DataTypeTemplate template, List<Type> typeHierarchy, Type type) {
		final Class<?> clazz = toClass(type);
		if (clazz == null) {
			throw extractionError("Not a class type.");
		}

		validateStructuredClass(clazz);

		final List<Field> fields = collectStructuredFields(clazz);

		if (fields.isEmpty()) {
			throw extractionError(
				"Class '%s' has no fields.",
				clazz.getName());
		}

		// if not all fields are mutable, a default constructor is not enough
		final boolean allFieldsMutable = fields.stream().allMatch(f -> {
			validateStructuredFieldReadability(clazz, f);
			return isStructuredFieldMutable(clazz, f);
		});

		final ExtractionUtils.AssigningConstructor constructor = extractAssigningConstructor(clazz, fields);
		if (!allFieldsMutable && constructor == null) {
			throw extractionError(
				"Class '%s' has immutable fields and thus requires a constructor that is publicly " +
					"accessible and assigns all fields: %s",
				clazz.getName(),
				fields.stream().map(Field::getName).collect(Collectors.joining(", ")));
		}

		final Map<String, DataType> fieldDataTypes = extractStructuredTypeFields(
			template,
			typeHierarchy,
			type,
			fields);

		final StructuredType.Builder builder = StructuredType.newBuilder(clazz);
		builder.attributes(createStructuredTypeAttributes(constructor, fieldDataTypes));
		builder.setFinal(true); // anonymous structured types should not allow inheritance
		builder.setInstantiable(true);
		return new FieldsDataType(builder.build(), clazz, fieldDataTypes);
	}

	private Map<String, DataType> extractStructuredTypeFields(
			DataTypeTemplate template,
			List<Type> typeHierarchy,
			Type type,
			List<Field> fields) {
		final Map<String, DataType> fieldDataTypes = new HashMap<>();
		final List<Type> structuredTypeHierarchy = collectTypeHierarchy(type);
		for (Field field : fields) {
			try {
				final Type fieldType = field.getGenericType();
				final List<Type> fieldTypeHierarchy = new ArrayList<>();
				// hierarchy until structured type
				fieldTypeHierarchy.addAll(typeHierarchy);
				// hierarchy of structured type
				fieldTypeHierarchy.addAll(structuredTypeHierarchy);
				final DataTypeTemplate fieldTemplate = mergeFieldTemplate(lookup, field, template);
				final DataType fieldDataType = extractDataTypeOrRaw(fieldTemplate, fieldTypeHierarchy, fieldType);
				fieldDataTypes.put(field.getName(), fieldDataType);
			} catch (Throwable t) {
				throw extractionError(
					t,
					"Error in field '%s' of class '%s'.",
					field.getName(),
					field.getDeclaringClass().getName());
			}
		}
		return fieldDataTypes;
	}

	private List<StructuredAttribute> createStructuredTypeAttributes(
			ExtractionUtils.AssigningConstructor constructor,
			Map<String, DataType> fieldDataTypes) {
		return Optional.ofNullable(constructor)
			.map(c -> {
				// field order is defined by assigning constructor
				return c.parameterNames.stream();
			})
			.orElseGet(() -> {
				// field order is sorted
				return fieldDataTypes.keySet().stream().sorted();
			})
			.map(name -> {
				final LogicalType logicalType = fieldDataTypes.get(name).getLogicalType();
				return new StructuredAttribute(name, logicalType);
			})
			.collect(Collectors.toList());
	}

	/**
	 * Merges the template of a structured type with a possibly more specific field annotation.
	 */
	private DataTypeTemplate mergeFieldTemplate(DataTypeLookup lookup, Field field, DataTypeTemplate structuredTemplate) {
		final DataTypeHint hint = field.getAnnotation(DataTypeHint.class);
		if (hint == null) {
			return structuredTemplate.copyWithoutDataType();
		}
		return structuredTemplate.mergeWithInnerAnnotation(lookup, hint);
	}

	/**
	 * Use closest class for data type if possible. Even though a hint might have provided some data
	 * type, in many cases, the conversion class can be enriched with the extraction type itself.
	 */
	@SuppressWarnings("unchecked")
	private DataType closestBridging(DataType dataType, @Nullable Class<?> clazz) {
		// no context class or conversion class is already more specific than context class
		if (clazz == null || clazz.isAssignableFrom(dataType.getConversionClass())) {
			return dataType;
		}
		final LogicalType logicalType = dataType.getLogicalType();
		final boolean supportsConversion = logicalType.supportsInputConversion(clazz) ||
			logicalType.supportsOutputConversion(clazz);
		if (supportsConversion && logicalType instanceof RawType) {
			return DataTypes.RAW(clazz, ((RawType) logicalType).getTypeSerializer());
		} else if (supportsConversion) {
			return dataType.bridgedTo(clazz);
		}
		return dataType;
	}
}
