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

package org.apache.flink.table.factories.datagen.types;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.streaming.api.functions.source.datagen.DataGenerator;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.util.Preconditions;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Generates random {@link DecimalData} values.
 */
public class DecimalDataRandomGenerator implements DataGenerator<DecimalData> {

	private final int precision;

	private final int scale;

	private final double min;

	private final double max;

	public DecimalDataRandomGenerator(int precision, int scale, double min, double max) {
		Preconditions.checkState(min < max, String.format("min bound must be less than max [%f, %f]", min, max));
		double largest = Math.pow(10, precision - scale) - Math.pow(10, -scale);
		this.precision = precision;
		this.scale = scale;
		this.min = Math.max(-1 * largest, min);
		this.max = Math.min(largest, max);
	}

	@Override
	public void open(String name, FunctionInitializationContext context, RuntimeContext runtimeContext) throws Exception {
	}

	@Override
	public boolean hasNext() {
		return true;
	}

	@Override
	public DecimalData next() {
		BigDecimal decimal = new BigDecimal(
			ThreadLocalRandom.current().nextDouble(min, max),
			new MathContext(precision, RoundingMode.DOWN));
		return DecimalData.fromBigDecimal(decimal, precision, scale);
	}
}
