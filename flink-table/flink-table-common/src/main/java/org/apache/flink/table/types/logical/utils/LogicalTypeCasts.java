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

package org.apache.flink.table.types.logical.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.table.types.logical.LogicalTypeFamily.BINARY_STRING;
import static org.apache.flink.table.types.logical.LogicalTypeFamily.CHARACTER_STRING;
import static org.apache.flink.table.types.logical.LogicalTypeFamily.CONSTRUCTED;
import static org.apache.flink.table.types.logical.LogicalTypeFamily.DATETIME;
import static org.apache.flink.table.types.logical.LogicalTypeFamily.EXACT_NUMERIC;
import static org.apache.flink.table.types.logical.LogicalTypeFamily.INTERVAL;
import static org.apache.flink.table.types.logical.LogicalTypeFamily.NUMERIC;
import static org.apache.flink.table.types.logical.LogicalTypeFamily.PREDEFINED;
import static org.apache.flink.table.types.logical.LogicalTypeFamily.TIME;
import static org.apache.flink.table.types.logical.LogicalTypeFamily.TIMESTAMP;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.ANY;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.BIGINT;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.BINARY;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.BOOLEAN;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.CHAR;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.DATE;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.DECIMAL;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.DISTINCT_TYPE;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.DOUBLE;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.FLOAT;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.INTEGER;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.INTERVAL_DAY_TIME;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.INTERVAL_YEAR_MONTH;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.NULL;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.SMALLINT;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.STRUCTURED_TYPE;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.SYMBOL;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.TIMESTAMP_WITH_TIME_ZONE;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.TINYINT;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.VARBINARY;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.VARCHAR;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.hasFamily;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.isSingleFieldInterval;

/**
 * Utilities for casting {@link LogicalType}.
 *
 * <p>This class aims to be compatible with the SQL standard. It is inspired by Apache Calcite's
 * {@code SqlTypeUtil#canCastFrom} method.
 *
 * <p>Casts can be performed in two ways: implicit or explicit.
 *
 * <p>Explicit casts correspond to the SQL cast specification and represent the logic behind a
 * {@code CAST(sourceType AS targetType)} operation. For example, it allows for converting most types
 * of the {@link LogicalTypeFamily#PREDEFINED} family to types of the {@link LogicalTypeFamily#CHARACTER_STRING}
 * family.
 *
 * <p>Implicit casts are used for safe type widening and type generalization (finding a common supertype
 * for a set of types) without loss of information. Implicit casts are similar to the Java semantics
 * (e.g. this is not possible: {@code int x = (String) z}).
 *
 * <p>Conversions that are defined by the {@link LogicalType} (e.g. interpreting a {@link DateType}
 * as integer value) are not considered here. They are an internal bridging feature that is not
 * standard compliant. If at all, {@code CONVERT} methods should make such conversions available.
 */
@Internal
public final class LogicalTypeCasts {

	private static final Map<LogicalTypeRoot, Set<LogicalTypeRoot>> implicitCastingRules;

	private static final Map<LogicalTypeRoot, Set<LogicalTypeRoot>> explicitCastingRules;

	static {
		implicitCastingRules = new HashMap<>();
		explicitCastingRules = new HashMap<>();

		// identity casts

		for (LogicalTypeRoot typeRoot : allTypes()) {
			castTo(typeRoot)
				.implicitFrom(typeRoot)
				.build();
		}

		// cast specification

		castTo(CHAR)
			.implicitFrom(CHAR)
			.explicitFromFamily(PREDEFINED)
			.explicitNotFromFamily(BINARY_STRING)
			.build();

		castTo(VARCHAR)
			.implicitFromFamily(CHARACTER_STRING)
			.explicitFromFamily(PREDEFINED)
			.explicitNotFromFamily(BINARY_STRING)
			.build();

		castTo(BOOLEAN)
			.implicitFrom(BOOLEAN)
			.explicitFromFamily(CHARACTER_STRING)
			.build();

		castTo(BINARY)
			.implicitFrom(BINARY)
			.build();

		castTo(VARBINARY)
			.implicitFromFamily(BINARY_STRING)
			.build();

		castTo(DECIMAL)
			.implicitFromFamily(NUMERIC)
			.explicitFromFamily(CHARACTER_STRING)
			.build();

		castTo(TINYINT)
			.implicitFrom(TINYINT)
			.explicitFromFamily(NUMERIC, CHARACTER_STRING)
			.build();

		castTo(SMALLINT)
			.implicitFrom(TINYINT, SMALLINT)
			.explicitFromFamily(NUMERIC, CHARACTER_STRING)
			.build();

		castTo(INTEGER)
			.implicitFrom(TINYINT, SMALLINT, INTEGER)
			.explicitFromFamily(NUMERIC, CHARACTER_STRING)
			.build();

		castTo(BIGINT)
			.implicitFrom(TINYINT, SMALLINT, INTEGER, BIGINT)
			.explicitFromFamily(NUMERIC, CHARACTER_STRING)
			.build();

		castTo(FLOAT)
			.implicitFrom(TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, DECIMAL)
			.explicitFromFamily(NUMERIC, CHARACTER_STRING)
			.build();

		castTo(DOUBLE)
			.implicitFromFamily(NUMERIC)
			.explicitFromFamily(CHARACTER_STRING)
			.build();

		castTo(DATE)
			.implicitFrom(DATE, TIMESTAMP_WITHOUT_TIME_ZONE)
			.explicitFromFamily(TIMESTAMP, CHARACTER_STRING)
			.build();

		castTo(TIME_WITHOUT_TIME_ZONE)
			.implicitFrom(TIME_WITHOUT_TIME_ZONE, TIMESTAMP_WITHOUT_TIME_ZONE)
			.explicitFromFamily(TIME, TIMESTAMP, CHARACTER_STRING)
			.build();

		castTo(TIMESTAMP_WITHOUT_TIME_ZONE)
			.implicitFrom(TIMESTAMP_WITHOUT_TIME_ZONE)
			.explicitFromFamily(DATETIME, CHARACTER_STRING)
			.build();

		castTo(TIMESTAMP_WITH_TIME_ZONE)
			.implicitFrom(TIMESTAMP_WITH_TIME_ZONE)
			.explicitFromFamily(DATETIME, CHARACTER_STRING)
			.build();

		castTo(TIMESTAMP_WITH_LOCAL_TIME_ZONE)
			.implicitFrom(TIMESTAMP_WITH_LOCAL_TIME_ZONE)
			.explicitFromFamily(DATETIME, CHARACTER_STRING)
			.build();

		castTo(INTERVAL_YEAR_MONTH)
			.implicitFrom(INTERVAL_YEAR_MONTH)
			.explicitFromFamily(EXACT_NUMERIC, CHARACTER_STRING)
			.build();

		castTo(INTERVAL_DAY_TIME)
			.implicitFrom(INTERVAL_DAY_TIME)
			.explicitFromFamily(EXACT_NUMERIC, CHARACTER_STRING)
			.build();
	}

	/**
	 * Returns whether the source type can be safely casted to the target type without loosing information.
	 *
	 * <p>Implicit casts are used for type widening and type generalization (finding a common supertype
	 * for a set of types). Implicit casts are similar to the Java semantics (e.g. this is not possible:
	 * {@code int x = (String) z}).
	 */
	public static boolean supportsImplicitCast(LogicalType sourceType, LogicalType targetType) {
		return supportsCasting(sourceType, targetType, false);
	}

	/**
	 * Returns whether the source type can be casted to the target type.
	 *
	 * <p>Explicit casts correspond to the SQL cast specification and represent the logic behind a
	 * {@code CAST(sourceType AS targetType)} operation. For example, it allows for converting most types
	 * of the {@link LogicalTypeFamily#PREDEFINED} family to types of the {@link LogicalTypeFamily#CHARACTER_STRING}
	 * family.
	 */
	public static boolean supportsExplicitCast(LogicalType sourceType, LogicalType targetType) {
		return supportsCasting(sourceType, targetType, true);
	}

	// --------------------------------------------------------------------------------------------

	private static boolean supportsCasting(
			LogicalType sourceType,
			LogicalType targetType,
			boolean allowExplicit) {
		if (sourceType.isNullable() && !targetType.isNullable()) {
			return false;
		}
		// ignore nullability during compare
		if (sourceType.copy(true).equals(targetType.copy(true))) {
			return true;
		}

		final LogicalTypeRoot sourceRoot = sourceType.getTypeRoot();
		final LogicalTypeRoot targetRoot = targetType.getTypeRoot();

		if (hasFamily(sourceType, INTERVAL) && hasFamily(targetType, EXACT_NUMERIC)) {
			// cast between interval and exact numeric is only supported if interval has a single field
			return isSingleFieldInterval(sourceType);
		} else if (hasFamily(sourceType, EXACT_NUMERIC) && hasFamily(targetType, INTERVAL)) {
			// cast between interval and exact numeric is only supported if interval has a single field
			return isSingleFieldInterval(targetType);
		} else if (hasFamily(sourceType, CONSTRUCTED) || hasFamily(targetType, CONSTRUCTED)) {
			return supportsConstructedCasting(sourceType, targetType, allowExplicit);
		} else if (sourceRoot == DISTINCT_TYPE && targetRoot == DISTINCT_TYPE) {
			// the two distinct types are not equal (from initial invariant), casting is not possible
			return false;
		} else if (sourceRoot == DISTINCT_TYPE) {
			return supportsCasting(((DistinctType) sourceType).getSourceType(), targetType, allowExplicit);
		} else if (targetRoot == DISTINCT_TYPE) {
			return supportsCasting(sourceType, ((DistinctType) targetType).getSourceType(), allowExplicit);
		} else if (sourceRoot == STRUCTURED_TYPE || targetRoot == STRUCTURED_TYPE) {
			// TODO structured types are not supported yet
			return false;
		} else if (sourceRoot == NULL) {
			// null can be cast to an arbitrary type
			return true;
		} else if (sourceRoot == ANY || targetRoot == ANY) {
			// the two any types are not equal (from initial invariant), casting is not possible
			return false;
		} else if (sourceRoot == SYMBOL || targetRoot == SYMBOL) {
			// the two symbol types are not equal (from initial invariant), casting is not possible
			return false;
		}

		if (implicitCastingRules.get(targetRoot).contains(sourceRoot)) {
			return true;
		}
		if (allowExplicit) {
			return explicitCastingRules.get(targetRoot).contains(sourceRoot);
		}
		return false;
	}

	private static boolean supportsConstructedCasting(
			LogicalType sourceType,
			LogicalType targetType,
			boolean allowExplicit) {
		final LogicalTypeRoot sourceRoot = sourceType.getTypeRoot();
		final LogicalTypeRoot targetRoot = targetType.getTypeRoot();
		// all constructed types can only be casted within the same type root
		if (sourceRoot == targetRoot) {
			final List<LogicalType> sourceChildren = sourceType.getChildren();
			final List<LogicalType> targetChildren = targetType.getChildren();
			if (sourceChildren.size() != targetChildren.size()) {
				return false;
			}
			for (int i = 0; i < sourceChildren.size(); i++) {
				if (!supportsCasting(sourceChildren.get(i), targetChildren.get(i), allowExplicit)) {
					return false;
				}
			}
			return true;
		}
		return false;
	}

	private static CastingRuleBuilder castTo(LogicalTypeRoot sourceType) {
		return new CastingRuleBuilder(sourceType);
	}

	private static LogicalTypeRoot[] allTypes() {
		return LogicalTypeRoot.values();
	}

	private static class CastingRuleBuilder {

		private final LogicalTypeRoot targetType;
		private Set<LogicalTypeRoot> implicitSourceTypes = new HashSet<>();
		private Set<LogicalTypeRoot> explicitSourceTypes = new HashSet<>();

		CastingRuleBuilder(LogicalTypeRoot targetType) {
			this.targetType = targetType;
		}

		CastingRuleBuilder implicitFrom(LogicalTypeRoot... sourceTypes) {
			this.implicitSourceTypes.addAll(Arrays.asList(sourceTypes));
			return this;
		}

		CastingRuleBuilder implicitFromFamily(LogicalTypeFamily... sourceFamilies) {
			for (LogicalTypeFamily family : sourceFamilies) {
				for (LogicalTypeRoot root : LogicalTypeRoot.values()) {
					if (root.getFamilies().contains(family)) {
						this.implicitSourceTypes.add(root);
					}
				}
			}
			return this;
		}

		CastingRuleBuilder explicitFrom(LogicalTypeRoot... sourceTypes) {
			this.explicitSourceTypes.addAll(Arrays.asList(sourceTypes));
			return this;
		}

		CastingRuleBuilder explicitFromFamily(LogicalTypeFamily... sourceFamilies) {
			for (LogicalTypeFamily family : sourceFamilies) {
				for (LogicalTypeRoot root : LogicalTypeRoot.values()) {
					if (root.getFamilies().contains(family)) {
						this.explicitSourceTypes.add(root);
					}
				}
			}
			return this;
		}

		/**
		 * Should be called after {@link #explicitFromFamily(LogicalTypeFamily...)} to remove previously
		 * added types.
		 */
		CastingRuleBuilder explicitNotFromFamily(LogicalTypeFamily... sourceFamilies) {
			for (LogicalTypeFamily family : sourceFamilies) {
				for (LogicalTypeRoot root : LogicalTypeRoot.values()) {
					if (root.getFamilies().contains(family)) {
						this.explicitSourceTypes.remove(root);
					}
				}
			}
			return this;
		}

		void build() {
			implicitCastingRules.put(targetType, implicitSourceTypes);
			explicitCastingRules.put(targetType, explicitSourceTypes);
		}
	}

	private LogicalTypeCasts() {
		// no instantiation
	}
}
