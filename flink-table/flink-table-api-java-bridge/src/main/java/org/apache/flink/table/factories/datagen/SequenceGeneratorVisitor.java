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
import org.apache.flink.streaming.api.functions.source.datagen.SequenceGenerator;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.factories.DataGenOptions;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarCharType;

import static org.apache.flink.configuration.ConfigOptions.key;

/** Creates a sequential {@link DataGeneratorContainer} for a particular logical type. */
@Internal
public class SequenceGeneratorVisitor extends DataGenVisitorBase {

    private final ReadableConfig config;

    private final String startKeyStr;

    private final String endKeyStr;

    private final ConfigOption<Integer> intStart;

    private final ConfigOption<Integer> intEnd;

    private final ConfigOption<Long> longStart;

    private final ConfigOption<Long> longEnd;

    public SequenceGeneratorVisitor(String name, ReadableConfig config) {
        super(name, config);

        this.config = config;

        this.startKeyStr = DataGenOptions.FIELDS + "." + name + "." + DataGenOptions.START;
        this.endKeyStr = DataGenOptions.FIELDS + "." + name + "." + DataGenOptions.END;

        ConfigOptions.OptionBuilder startKey = key(startKeyStr);
        ConfigOptions.OptionBuilder endKey = key(endKeyStr);

        config.getOptional(startKey.stringType().noDefaultValue())
                .orElseThrow(
                        () ->
                                new ValidationException(
                                        "Could not find required property '"
                                                + startKeyStr
                                                + "' for sequence generator."));
        config.getOptional(endKey.stringType().noDefaultValue())
                .orElseThrow(
                        () ->
                                new ValidationException(
                                        "Could not find required property '"
                                                + endKeyStr
                                                + "' for sequence generator."));

        this.intStart = startKey.intType().noDefaultValue();
        this.intEnd = endKey.intType().noDefaultValue();
        this.longStart = startKey.longType().noDefaultValue();
        this.longEnd = endKey.longType().noDefaultValue();
    }

    @Override
    public DataGeneratorContainer visit(BooleanType booleanType) {
        return DataGeneratorContainer.of(RandomGenerator.booleanGenerator());
    }

    @Override
    public DataGeneratorContainer visit(CharType booleanType) {
        return DataGeneratorContainer.of(
                getSequenceStringGenerator(config.get(longStart), config.get(longEnd)),
                longStart,
                longEnd);
    }

    @Override
    public DataGeneratorContainer visit(VarCharType booleanType) {
        return DataGeneratorContainer.of(
                getSequenceStringGenerator(config.get(longStart), config.get(longEnd)),
                longStart,
                longEnd);
    }

    @Override
    public DataGeneratorContainer visit(TinyIntType booleanType) {
        return DataGeneratorContainer.of(
                SequenceGenerator.byteGenerator(
                        config.get(intStart).byteValue(), config.get(intEnd).byteValue()),
                intStart,
                intEnd);
    }

    @Override
    public DataGeneratorContainer visit(SmallIntType booleanType) {
        return DataGeneratorContainer.of(
                SequenceGenerator.shortGenerator(
                        config.get(intStart).shortValue(), config.get(intEnd).shortValue()),
                intStart,
                intEnd);
    }

    @Override
    public DataGeneratorContainer visit(IntType integerType) {
        return DataGeneratorContainer.of(
                SequenceGenerator.intGenerator(config.get(intStart), config.get(intEnd)),
                intStart,
                intEnd);
    }

    @Override
    public DataGeneratorContainer visit(BigIntType bigIntType) {
        return DataGeneratorContainer.of(
                SequenceGenerator.longGenerator(config.get(longStart), config.get(longEnd)),
                longStart,
                longEnd);
    }

    @Override
    public DataGeneratorContainer visit(FloatType floatType) {
        return DataGeneratorContainer.of(
                SequenceGenerator.floatGenerator(
                        config.get(intStart).shortValue(), config.get(intEnd).shortValue()),
                intStart,
                intEnd);
    }

    @Override
    public DataGeneratorContainer visit(DoubleType doubleType) {
        return DataGeneratorContainer.of(
                SequenceGenerator.doubleGenerator(config.get(intStart), config.get(intEnd)),
                intStart,
                intEnd);
    }

    @Override
    public DataGeneratorContainer visit(DecimalType decimalType) {
        return DataGeneratorContainer.of(
                SequenceGenerator.bigDecimalGenerator(
                        config.get(intStart),
                        config.get(intEnd),
                        decimalType.getPrecision(),
                        decimalType.getScale()),
                intStart,
                intEnd);
    }

    private static SequenceGenerator<StringData> getSequenceStringGenerator(long start, long end) {
        return new SequenceGenerator<StringData>(start, end) {
            @Override
            public StringData next() {
                return StringData.fromString(valuesToEmit.poll().toString());
            }
        };
    }
}
