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

package org.apache.flink.table.factories.datagen;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.functions.source.datagen.RandomGenerator;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.utils.LogicalTypeDefaultVisitor;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.table.factories.DataGenTableSourceFactory.*;

/**
 * Creates a random {@link DataGeneratorContainer} for a particular logical type.
 */
@Internal
public class RandomGeneratorVisitor extends LogicalTypeDefaultVisitor<DataGeneratorContainer> {

	public static final int RANDOM_STRING_LENGTH_DEFAULT = 100;

	private final String name;

	private final DataType type;

	private final ReadableConfig config;

	private final ConfigOptions.OptionBuilder minKey;

	private final ConfigOptions.OptionBuilder maxKey;

	public RandomGeneratorVisitor(String name, DataType type, ReadableConfig config) {
		this.name = name;
		this.type = type;
		this.config = config;

		this.minKey = key(FIELDS + "." + name + "." + MIN);
		this.maxKey = key(FIELDS + "." + name + "." + MAX);
	}

	@Override
	public DataGeneratorContainer visit(BooleanType booleanType) {
		return DataGeneratorContainer.of(RandomGenerator.booleanGenerator());
	}

	@Override
	public DataGeneratorContainer visit(CharType booleanType) {
		ConfigOption<Integer> lenOption = key(FIELDS + "." + name + "." + LENGTH)
			.intType()
			.defaultValue(RANDOM_STRING_LENGTH_DEFAULT);
		return DataGeneratorContainer.of(getRandomStringGenerator(config.get(lenOption)), lenOption);
	}

	@Override
	public DataGeneratorContainer visit(VarCharType booleanType) {
		ConfigOption<Integer> lenOption = key(FIELDS + "." + name + "." + LENGTH)
			.intType()
			.defaultValue(RANDOM_STRING_LENGTH_DEFAULT);
		return DataGeneratorContainer.of(getRandomStringGenerator(config.get(lenOption)), lenOption);
	}

	@Override
	public DataGeneratorContainer visit(TinyIntType booleanType) {
		ConfigOption<Integer> min = minKey.intType().defaultValue((int) Byte.MIN_VALUE);
		ConfigOption<Integer> max = maxKey.intType().defaultValue((int) Byte.MAX_VALUE);
		return DataGeneratorContainer.of(
			RandomGenerator.byteGenerator(
				config.get(min).byteValue(), config.get(max).byteValue()),
			min, max);
	}

	@Override
	public DataGeneratorContainer visit(SmallIntType booleanType) {
		ConfigOption<Integer> min = minKey.intType().defaultValue((int) Short.MIN_VALUE);
		ConfigOption<Integer> max = maxKey.intType().defaultValue((int) Short.MAX_VALUE);
		return DataGeneratorContainer.of(
			RandomGenerator.shortGenerator(
				config.get(min).shortValue(),
				config.get(max).shortValue()),
			min, max);
	}

	@Override
	public DataGeneratorContainer visit(IntType integerType) {
		ConfigOption<Integer> min = minKey.intType().defaultValue(Integer.MIN_VALUE);
		ConfigOption<Integer> max = maxKey.intType().defaultValue(Integer.MAX_VALUE);
		return DataGeneratorContainer.of(
			RandomGenerator.intGenerator(
				config.get(min), config.get(max)),
			min, max);
	}

	@Override
	public DataGeneratorContainer visit(BigIntType bigIntType) {
		ConfigOption<Long> min = minKey.longType().defaultValue(Long.MIN_VALUE);
		ConfigOption<Long> max = maxKey.longType().defaultValue(Long.MAX_VALUE);
		return DataGeneratorContainer.of(
			RandomGenerator.longGenerator(
				config.get(min), config.get(max)),
			min, max);
	}

	@Override
	public DataGeneratorContainer visit(FloatType floatType) {
		ConfigOption<Float> min = minKey.floatType().defaultValue(Float.MIN_VALUE);
		ConfigOption<Float> max = maxKey.floatType().defaultValue(Float.MAX_VALUE);
		return DataGeneratorContainer.of(
			RandomGenerator.floatGenerator(
				config.get(min), config.get(max)),
			min, max);
	}

	@Override
	public DataGeneratorContainer visit(DoubleType doubleType) {
		ConfigOption<Double> min = minKey.doubleType().defaultValue(Double.MIN_VALUE);
		ConfigOption<Double> max = maxKey.doubleType().defaultValue(Double.MAX_VALUE);
		return DataGeneratorContainer.of(
			RandomGenerator.doubleGenerator(
				config.get(min), config.get(max)),
			min, max);
	}

	@Override
	protected DataGeneratorContainer defaultMethod(LogicalType logicalType) {
		throw new ValidationException("Unsupported type: " + type);
	}

	private static RandomGenerator<StringData> getRandomStringGenerator(int length) {
		return new RandomGenerator<StringData>() {
			@Override
			public StringData next() {
				return StringData.fromString(random.nextHexString(length));
			}
		};
	}
}
