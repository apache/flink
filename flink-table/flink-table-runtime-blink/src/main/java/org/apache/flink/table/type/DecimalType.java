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

package org.apache.flink.table.type;

import org.apache.flink.table.typeutils.DecimalTypeInfo;

import java.math.BigDecimal;

import static java.lang.String.format;

/**
 * Decimal type.
 */
public class DecimalType implements AtomicType {

	private static final long serialVersionUID = 1L;

	public static final int MAX_PRECISION = 38;

	public static final int MAX_COMPACT_PRECISION = 18;

	private final int precision;
	private final int scale;

	// Default to be same with Integer.
	public static final DecimalType USER_DEFAULT = new DecimalType(10, 0);

	// Mainly used for implicitly type cast and test.
	public static final DecimalType SYSTEM_DEFAULT = new DecimalType(MAX_PRECISION, 18);

	public DecimalType(int precision, int scale) {
		if (precision < 0) {
			throw new IllegalArgumentException(format("Decimal precision (%s) cannot be negative.",
					precision));
		}

		if (scale > precision) {
			throw new IllegalArgumentException(format("Decimal scale (%s) cannot be greater than " +
					"precision (%s).", scale, precision));
		}

		if (precision > MAX_PRECISION) {
			throw new IllegalArgumentException(
					"DecimalType can only support precision up to " + MAX_PRECISION);
		}

		this.precision = precision;
		this.scale = scale;
	}

	public int precision() {
		return this.precision;
	}

	public int scale() {
		return this.scale;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		DecimalType that = (DecimalType) o;

		return precision == that.precision && scale == that.scale;
	}

	@Override
	public int hashCode() {
		int result = precision;
		result = 31 * result + scale;
		return result;
	}

	@Override
	public String toString() {
		return "DecimalType{" +
				"precision=" + precision +
				", scale=" + scale +
				'}';
	}

	public DecimalTypeInfo toTypeInfo() {
		return new DecimalTypeInfo(precision, scale);
	}

	public static DecimalType of(int precision, int scale) {
		return new DecimalType(precision, scale);
	}

	public static DecimalType of(BigDecimal value) {
		return of(value.precision(), value.scale());
	}

	/**
	 * https://docs.microsoft.com/en-us/sql/t-sql/data-types/precision-scale-and-length-transact-sql.
	 */
	public static DecimalType inferDivisionType(int precision1, int scale1, int precision2, int scale2) {
		// note: magic numbers are used directly here, because it's not really a general algorithm.
		int scale = Math.max(6, scale1 + precision2 + 1);
		int precision = precision1 - scale1 + scale2 + scale;
		if (precision > 38) {
			scale = Math.max(6, 38 - (precision - scale));
			precision = 38;
		}
		return new DecimalType(precision, scale);
	}

	public static DecimalType inferIntDivType(int precision1, int scale1, int scale2) {
		int p = Math.min(38, precision1 - scale1 + scale2);
		return new DecimalType(p, 0);
	}

	/**
	 * https://docs.microsoft.com/en-us/sql/t-sql/functions/sum-transact-sql.
	 */
	public static DecimalType inferAggSumType(int scale) {
		return new DecimalType(38, scale);
	}

	/**
	 * https://docs.microsoft.com/en-us/sql/t-sql/functions/avg-transact-sql
	 * however, we count by LONG, therefore divide by Decimal(20,0),
	 * but the end result is actually the same, which is Decimal(38, max(6,s)).
	 */
	public static DecimalType inferAggAvgType(int scale) {
		return inferDivisionType(38, scale, 20, 0);
	}

	/**
	 * return type of Round( DECIMAL(p,s), r).
	 */
	public static DecimalType inferRoundType(int precision, int scale, int r) {
		if (r >= scale) {
			return new DecimalType(precision, scale);
		} else if (r < 0) {
			return new DecimalType(Math.min(38, 1 + precision - scale), 0);
		} else { // 0 <= r < s
			return new DecimalType(1 + precision - scale + r, r);
		}
		// NOTE: rounding may increase the digits by 1, therefore we need +1 on precisions.
	}
}
