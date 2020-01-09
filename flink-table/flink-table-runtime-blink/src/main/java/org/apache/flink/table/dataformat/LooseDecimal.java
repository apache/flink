/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.	See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.	You may obtain a copy of the License at
 *
 *		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.dataformat;

import java.math.BigDecimal;

/**
 * Sql Decimal value based on a Long.
 */
public class LooseDecimal extends Decimal {

	BigDecimal decimalVal;

	public LooseDecimal(int precision, int scale, BigDecimal decimalVal) {
		super(precision, scale);
		this.decimalVal = decimalVal;
	}

	@Override
	public boolean isCompact() {
		return false;
	}

	@Override
	public BigDecimal toBigDecimal() {
		return decimalVal;
	}

	@Override
	public int hashCode() {
		return decimalVal.hashCode();
	}

	@Override
	public boolean equals(final Object o) {
		if (!(o instanceof LooseDecimal)) {
			return false;
		}
		LooseDecimal that = (LooseDecimal) o;
		return this.toBigDecimal().compareTo(that.toBigDecimal()) == 0;
	}

	@Override
	public int signum() {
		return decimalVal.signum();
	}

	@Override
	public LooseDecimal copy() {
		return new LooseDecimal(precision, scale, decimalVal);
	}

	@Override
	public byte[] toUnscaledBytes() {
		return toBigDecimal().unscaledValue().toByteArray();
	}

	@Override
	public double doubleValue() {
		return decimalVal.doubleValue();
	}

	@Override
	public Decimal negate() {
		return new LooseDecimal(precision, scale, decimalVal.negate());
	}

	@Override
	public Decimal abs() {
		if (decimalVal.signum() >= 0) {
			return this;
		} else {
			return new LooseDecimal(precision, scale, decimalVal.negate());
		}
	}
}
