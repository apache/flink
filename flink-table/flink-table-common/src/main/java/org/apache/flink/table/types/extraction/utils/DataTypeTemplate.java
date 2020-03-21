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

package org.apache.flink.table.types.extraction.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ExtractionVersion;
import org.apache.flink.table.annotation.HintFlag;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.extraction.DataTypeExtractor;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Objects;
import java.util.function.Function;

import static org.apache.flink.table.types.extraction.utils.ExtractionUtils.createRawType;

/**
 * Internal representation of a {@link DataTypeHint}.
 *
 * <p>All parameters of a template are optional. An empty annotation results in a template where all
 * members are {@code null}.
 */
@Internal
public final class DataTypeTemplate {

	private static final DataTypeHint DEFAULT_ANNOTATION = getDefaultAnnotation();

	private static final String RAW_TYPE_NAME = "RAW";

	public final @Nullable DataType dataType;

	public final @Nullable Class<? extends TypeSerializer<?>> rawSerializer;

	public final @Nullable InputGroup inputGroup;

	public final @Nullable ExtractionVersion version;

	public final @Nullable Boolean allowRawGlobally;

	public final @Nullable String[] allowRawPattern;

	public final @Nullable String[] forceRawPattern;

	public final @Nullable Integer defaultDecimalPrecision;

	public final @Nullable Integer defaultDecimalScale;

	public final @Nullable Integer defaultYearPrecision;

	public final @Nullable Integer defaultSecondPrecision;

	private DataTypeTemplate(
			@Nullable DataType dataType,
			@Nullable Class<? extends TypeSerializer<?>> rawSerializer,
			@Nullable InputGroup inputGroup,
			@Nullable ExtractionVersion version,
			@Nullable Boolean allowRawGlobally,
			@Nullable String[] allowRawPattern,
			@Nullable String[] forceRawPattern,
			@Nullable Integer defaultDecimalPrecision,
			@Nullable Integer defaultDecimalScale,
			@Nullable Integer defaultYearPrecision,
			@Nullable Integer defaultSecondPrecision) {
		this.dataType = dataType;
		this.rawSerializer = rawSerializer;
		this.inputGroup = inputGroup;
		this.version = version;
		this.allowRawGlobally = allowRawGlobally;
		this.allowRawPattern = allowRawPattern;
		this.forceRawPattern = forceRawPattern;
		this.defaultDecimalPrecision = defaultDecimalPrecision;
		this.defaultDecimalScale = defaultDecimalScale;
		this.defaultYearPrecision = defaultYearPrecision;
		this.defaultSecondPrecision = defaultSecondPrecision;
	}

	/**
	 * Creates an instance from the given {@link DataTypeHint}. Resolves an explicitly defined data type
	 * if {@link DataTypeHint#value()} and/or {@link DataTypeHint#bridgedTo()} are defined.
	 */
	public static DataTypeTemplate fromAnnotation(DataTypeFactory typeFactory, DataTypeHint hint) {
		final String typeName = defaultAsNull(hint, DataTypeHint::value);
		final Class<?> conversionClass = defaultAsNull(hint, DataTypeHint::bridgedTo);
		if (typeName != null || conversionClass != null) {
			// chicken and egg problem
			// a template can contain a data type but in order to extract a data type we might need a template
			final DataTypeTemplate extractionTemplate = fromAnnotation(hint, null);
			return fromAnnotation(hint, extractDataType(typeFactory, typeName, conversionClass, extractionTemplate));
		}
		return fromAnnotation(hint, null);
	}

	/**
	 * Creates an instance from the given {@link DataTypeHint} with a resolved data type if available.
	 */
	public static DataTypeTemplate fromAnnotation(DataTypeHint hint, @Nullable DataType dataType) {
		return new DataTypeTemplate(
			dataType,
			defaultAsNull(hint, DataTypeHint::rawSerializer),
			defaultAsNull(hint, DataTypeHint::inputGroup),
			defaultAsNull(hint, DataTypeHint::version),
			hintFlagToBoolean(defaultAsNull(hint, DataTypeHint::allowRawGlobally)),
			defaultAsNull(hint, DataTypeHint::allowRawPattern),
			defaultAsNull(hint, DataTypeHint::forceRawPattern),
			defaultAsNull(hint, DataTypeHint::defaultDecimalPrecision),
			defaultAsNull(hint, DataTypeHint::defaultDecimalScale),
			defaultAsNull(hint, DataTypeHint::defaultYearPrecision),
			defaultAsNull(hint, DataTypeHint::defaultSecondPrecision)
		);
	}

	/**
	 * Creates an instance with no parameter content.
	 */
	public static DataTypeTemplate fromDefaults() {
		return new DataTypeTemplate(
			null,
			null,
			null,
			null,
			null,
			null,
			null,
			null,
			null,
			null,
			null
		);
	}

	/**
	 * Copies this template but removes the explicit data type (if available).
	 */
	public DataTypeTemplate copyWithoutDataType() {
		return new DataTypeTemplate(
			null,
			rawSerializer,
			inputGroup,
			version,
			allowRawGlobally,
			allowRawPattern,
			forceRawPattern,
			defaultDecimalPrecision,
			defaultDecimalScale,
			defaultYearPrecision,
			defaultSecondPrecision
		);
	}

	/**
	 * Merges this template with an inner annotation. The inner annotation has highest precedence
	 * and definitely determines the explicit data type (if available).
	 */
	public DataTypeTemplate mergeWithInnerAnnotation(DataTypeFactory typeFactory, DataTypeHint hint) {
		final DataTypeTemplate otherTemplate = fromAnnotation(typeFactory, hint);
		return new DataTypeTemplate(
			otherTemplate.dataType,
			rightValueIfNotNull(rawSerializer, otherTemplate.rawSerializer),
			rightValueIfNotNull(inputGroup, otherTemplate.inputGroup),
			rightValueIfNotNull(version, otherTemplate.version),
			rightValueIfNotNull(allowRawGlobally, otherTemplate.allowRawGlobally),
			rightValueIfNotNull(allowRawPattern, otherTemplate.allowRawPattern),
			rightValueIfNotNull(forceRawPattern, otherTemplate.forceRawPattern),
			rightValueIfNotNull(defaultDecimalPrecision, otherTemplate.defaultDecimalPrecision),
			rightValueIfNotNull(defaultDecimalScale, otherTemplate.defaultDecimalScale),
			rightValueIfNotNull(defaultYearPrecision, otherTemplate.defaultYearPrecision),
			rightValueIfNotNull(defaultSecondPrecision, otherTemplate.defaultSecondPrecision)
		);
	}

	/**
	 * Returns whether RAW types are allowed everywhere.
	 */
	public boolean isAllowRawGlobally() {
		return allowRawGlobally != null && allowRawGlobally;
	}

	/**
	 * Returns whether the given class is eligible for being treated as RAW type.
	 */
	public boolean isAllowAnyPattern(@Nullable Class<?> clazz) {
		if (allowRawPattern == null || clazz == null) {
			return false;
		}
		final String className = clazz.getName();
		for (String pattern : allowRawPattern) {
			if (className.startsWith(pattern)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Returns whether the given class must be treated as RAW type.
	 */
	public boolean isForceAnyPattern(@Nullable Class<?> clazz) {
		if (forceRawPattern == null || clazz == null) {
			return false;
		}
		final String className = clazz.getName();
		for (String pattern : forceRawPattern) {
			if (className.startsWith(pattern)) {
				return true;
			}
		}
		return false;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		DataTypeTemplate that = (DataTypeTemplate) o;
		return Objects.equals(dataType, that.dataType) &&
			Objects.equals(rawSerializer, that.rawSerializer) &&
			Objects.equals(inputGroup, that.inputGroup) &&
			version == that.version &&
			Objects.equals(allowRawGlobally, that.allowRawGlobally) &&
			Arrays.equals(allowRawPattern, that.allowRawPattern) &&
			Arrays.equals(forceRawPattern, that.forceRawPattern) &&
			Objects.equals(defaultDecimalPrecision, that.defaultDecimalPrecision) &&
			Objects.equals(defaultDecimalScale, that.defaultDecimalScale) &&
			Objects.equals(defaultYearPrecision, that.defaultYearPrecision) &&
			Objects.equals(defaultSecondPrecision, that.defaultSecondPrecision);
	}

	@Override
	public int hashCode() {
		int result = Objects.hash(
			dataType,
			rawSerializer,
			inputGroup,
			version,
			allowRawGlobally,
			defaultDecimalPrecision,
			defaultDecimalScale,
			defaultYearPrecision,
			defaultSecondPrecision);
		result = 31 * result + Arrays.hashCode(allowRawPattern);
		result = 31 * result + Arrays.hashCode(forceRawPattern);
		return result;
	}

	// --------------------------------------------------------------------------------------------

	@DataTypeHint
	private static class DefaultAnnotationHelper {
		// no implementation
	}

	private static DataTypeHint getDefaultAnnotation() {
		return DefaultAnnotationHelper.class.getAnnotation(DataTypeHint.class);
	}

	private static <T> T defaultAsNull(DataTypeHint hint, Function<DataTypeHint, T> accessor) {
		final T defaultValue = accessor.apply(DEFAULT_ANNOTATION);
		final T actualValue = accessor.apply(hint);
		if (Objects.deepEquals(defaultValue, actualValue)) {
			return null;
		}
		return actualValue;
	}

	private static <T> T rightValueIfNotNull(T l, T r) {
		if (r != null) {
			return r;
		}
		return l;
	}

	private static Boolean hintFlagToBoolean(HintFlag flag) {
		if (flag == null) {
			return null;
		}
		return flag == HintFlag.TRUE;
	}

	private static DataType extractDataType(
			DataTypeFactory typeFactory,
			@Nullable String typeName,
			@Nullable Class<?> conversionClass,
			DataTypeTemplate template) {
		// explicit data type
		if (typeName != null) {
			// RAW type
			if (typeName.equals(RAW_TYPE_NAME)) {
				return createRawType(typeFactory, template.rawSerializer, conversionClass);
			}
			// regular type that must be resolvable
			final DataType resolvedDataType = typeFactory.createDataType(typeName);
			if (conversionClass != null) {
				return resolvedDataType.bridgedTo(conversionClass);
			}
			return resolvedDataType;
		}
		// extracted data type
		else if (conversionClass != null) {
			return DataTypeExtractor.extractFromType(typeFactory, template, conversionClass);
		}
		throw ExtractionUtils.extractionError(
			"Data type hint does neither specify an explicit data type or conversion class " +
				"from which a data type could be extracted.");
	}
}
