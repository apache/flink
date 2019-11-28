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

package org.apache.flink.table.runtime.typeutils;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.base.BigDecComparator;
import org.apache.flink.api.common.typeutils.base.BigDecSerializer;

import java.math.BigDecimal;
import java.util.Arrays;

/**
 * {@link TypeInformation} for {@link BigDecimal}.
 *
 * <p>It differs from {@link BasicTypeInfo#BIG_DEC_TYPE_INFO} in that:
 * This type includes `precision` and `scale`, similar to SQL DECIMAL.
 */
public class BigDecimalTypeInfo extends BasicTypeInfo<BigDecimal> {

	private static final long serialVersionUID = 1L;

	public static BigDecimalTypeInfo of(int precision, int scale) {
		return new BigDecimalTypeInfo(precision, scale);
	}

	public static BigDecimalTypeInfo of(BigDecimal value) {
		return of(value.precision(), value.scale());
	}

	private final int precision;

	private final int scale;

	public BigDecimalTypeInfo(int precision, int scale) {
		super(BigDecimal.class, new Class<?>[]{}, BigDecSerializer.INSTANCE, BigDecComparator.class);
		this.precision = precision;
		this.scale = scale;
	}

	@Override
	public String toString() {
		return String.format("Decimal(%d,%d)", precision(), scale());
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof BigDecimalTypeInfo)) {
			return false;
		}
		BigDecimalTypeInfo that = (BigDecimalTypeInfo) obj;
		return this.precision() == that.precision() && this.scale() == that.scale();
	}

	@Override
	public int hashCode() {
		int h0 = this.getClass().getCanonicalName().hashCode();
		return Arrays.hashCode(new int[]{h0, precision(), scale()});
	}

	@Override
	public boolean shouldAutocastTo(BasicTypeInfo<?> to) {
		return (to.getTypeClass() == BigDecimal.class)
			|| super.shouldAutocastTo(to);
	}

	public int precision() {
		return precision;
	}

	public int scale() {
		return scale;
	}
}

