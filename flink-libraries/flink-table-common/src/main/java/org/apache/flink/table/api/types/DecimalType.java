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

package org.apache.flink.table.api.types;

import java.math.BigDecimal;

import static java.lang.String.format;

/**
 * Decimal type.
 */
public class DecimalType extends AtomicType {

	public static final int MAX_PRECISION = 38;

	public static final int MAX_SCALE = 18;

	private final int precision;
	private final int scale;
	// Default to be same with Integer.
	public static final DecimalType USER_DEFAULT = new DecimalType(10, 0);

	// Mainly used for implicitly type cast and test.
	public static final DecimalType SYSTEM_DEFAULT = new DecimalType(MAX_PRECISION, MAX_SCALE);

	public DecimalType(int precision, int scale) {

		if (scale > precision) {
			throw new IllegalArgumentException(format("Decimal scale (%s) cannot be greater than " +
					"precision (%s).", scale, precision));
		}

		if (precision > DecimalType.MAX_PRECISION) {
			throw new IllegalArgumentException("DecimalType can only support precision up to 38");
		}

		this.precision = precision;
		this.scale = scale;
	}

	public int precision(){
		return this.precision;
	}

	public int scale(){
		return this.scale;
	}

	public static DecimalType of(int precision, int scale) {
		return new DecimalType(precision, scale);
	}

	public static DecimalType of(BigDecimal value) {
		return of(value.precision(), value.scale());
	}

	/**
	 * Create a DecimalType instance from its qualified name in the form of "decimal(p,s)".
	 * @param qualifiedName qualified decimal type name
	 * @return a DecimalType instance
	 */
	public static DecimalType of(String qualifiedName) {
		if (!qualifiedName.startsWith("decimal(")) {
			throw new IllegalArgumentException("Illegal form of qualified decimal type " + qualifiedName);
		}

		int start = qualifiedName.indexOf('(');
		int end = qualifiedName.indexOf(')');
		String ps = qualifiedName.substring(start + 1, end);
		String[] sArray = ps.split(",");
		if (sArray.length != 2) {
			throw new IllegalArgumentException("Illegal form of qualified decimal type " + qualifiedName);
		}

		int precision = Integer.parseInt(sArray[0]);
		int scale = Integer.parseInt(sArray[1]);
		return new DecimalType(precision, scale);
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
		int result = super.hashCode();
		result = 31 * result + precision;
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
}
