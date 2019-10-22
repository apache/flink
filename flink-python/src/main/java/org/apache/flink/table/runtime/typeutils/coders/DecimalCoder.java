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

package org.apache.flink.table.runtime.typeutils.coders;

import org.apache.flink.table.dataformat.Decimal;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;

/**
 * A {@link Coder} for {@link Decimal}.
 */
public class DecimalCoder extends Coder<Decimal> {

	private static final long serialVersionUID = 1L;

	public static DecimalCoder of(int precision, int scale) {
		return new DecimalCoder(precision, scale);
	}

	private static final BigDecimalCoder BIG_DECIMAL_CODER = BigDecimalCoder.of();

	private final int precision;

	private final int scale;

	private DecimalCoder(int precision, int scale) {
		this.precision = precision;
		this.scale = scale;
	}

	@Override
	public void encode(Decimal value, OutputStream outStream) throws IOException {
		if (value == null) {
			throw new CoderException("Cannot encode a null Decimal for DecimalCoder");
		}
		BIG_DECIMAL_CODER.encode(value.toBigDecimal(), outStream);
	}

	@Override
	public Decimal decode(InputStream inStream) throws IOException {
		return Decimal.fromBigDecimal(BIG_DECIMAL_CODER.decode(inStream), precision, scale);
	}

	@Override
	public List<? extends Coder<?>> getCoderArguments() {
		return Collections.emptyList();
	}

	@Override
	public void verifyDeterministic() {
	}
}
