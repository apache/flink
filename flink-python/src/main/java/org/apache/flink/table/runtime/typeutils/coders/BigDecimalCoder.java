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

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.StringUtf8Coder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;

/**
 * A {@link Coder} for {@link BigDecimal}.
 */
public class BigDecimalCoder extends Coder<BigDecimal> {

	public static BigDecimalCoder of() {
		return INSTANCE;
	}

	private static final BigDecimalCoder INSTANCE = new BigDecimalCoder();

	private static final StringUtf8Coder STRING_CODER = StringUtf8Coder.of();

	private BigDecimalCoder() {
	}

	@Override
	public void encode(BigDecimal value, OutputStream outStream) throws IOException {
		if (value == null) {
			throw new CoderException("Cannot encode a null BigDecimal for BigDecimalCoder");
		}
		STRING_CODER.encode(value.toString(), outStream, Context.NESTED);
	}

	@Override
	public BigDecimal decode(InputStream inStream) throws IOException {
		return new BigDecimal(STRING_CODER.decode(inStream, Context.NESTED));
	}

	@Override
	public List<? extends Coder<?>> getCoderArguments() {
		return Collections.emptyList();
	}

	@Override
	public void verifyDeterministic() {
	}
}
