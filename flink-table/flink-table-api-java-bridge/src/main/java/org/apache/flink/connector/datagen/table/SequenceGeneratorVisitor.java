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

package org.apache.flink.connector.datagen.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.datagen.table.types.RandomGeneratorFunction;
import org.apache.flink.connector.datagen.table.types.SequenceGeneratorFunction;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
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

        this.startKeyStr =
                DataGenConnectorOptionsUtil.FIELDS
                        + "."
                        + name
                        + "."
                        + DataGenConnectorOptionsUtil.START;
        this.endKeyStr =
                DataGenConnectorOptionsUtil.FIELDS
                        + "."
                        + name
                        + "."
                        + DataGenConnectorOptionsUtil.END;

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
        return DataGeneratorContainer.of(RandomGeneratorFunction.booleanGenerator());
    }

    @Override
    public DataGeneratorContainer visit(CharType charType) {
        return DataGeneratorContainer.of(
                SequenceGeneratorFunction.stringDataGenerator(
                        config.get(longStart), config.get(longEnd)),
                longStart,
                longEnd);
    }

    @Override
    public DataGeneratorContainer visit(VarCharType varCharType) {
        return DataGeneratorContainer.of(
                SequenceGeneratorFunction.stringDataGenerator(
                        config.get(longStart), config.get(longEnd)),
                longStart,
                longEnd);
    }

    @Override
    public DataGeneratorContainer visit(BinaryType binaryType) {
        return DataGeneratorContainer.of(
                SequenceGeneratorFunction.bytesGenerator(
                        config.get(longStart), config.get(longEnd)),
                longStart,
                longEnd);
    }

    @Override
    public DataGeneratorContainer visit(VarBinaryType varBinaryType) {
        return DataGeneratorContainer.of(
                SequenceGeneratorFunction.bytesGenerator(
                        config.get(longStart), config.get(longEnd)),
                longStart,
                longEnd);
    }

    @Override
    public DataGeneratorContainer visit(TinyIntType tinyIntType) {
        return DataGeneratorContainer.of(
                SequenceGeneratorFunction.byteGenerator(
                        config.get(intStart).byteValue(), config.get(intEnd).byteValue()),
                intStart,
                intEnd);
    }

    @Override
    public DataGeneratorContainer visit(SmallIntType smallIntType) {
        return DataGeneratorContainer.of(
                SequenceGeneratorFunction.shortGenerator(
                        config.get(intStart).shortValue(), config.get(intEnd).shortValue()),
                intStart,
                intEnd);
    }

    @Override
    public DataGeneratorContainer visit(IntType integerType) {
        return DataGeneratorContainer.of(
                SequenceGeneratorFunction.intGenerator(config.get(intStart), config.get(intEnd)),
                intStart,
                intEnd);
    }

    @Override
    public DataGeneratorContainer visit(BigIntType bigIntType) {
        return DataGeneratorContainer.of(
                SequenceGeneratorFunction.longGenerator(config.get(longStart), config.get(longEnd)),
                longStart,
                longEnd);
    }

    @Override
    public DataGeneratorContainer visit(FloatType floatType) {
        return DataGeneratorContainer.of(
                SequenceGeneratorFunction.floatGenerator(
                        config.get(intStart).shortValue(), config.get(intEnd).shortValue()),
                intStart,
                intEnd);
    }

    @Override
    public DataGeneratorContainer visit(DoubleType doubleType) {
        return DataGeneratorContainer.of(
                SequenceGeneratorFunction.doubleGenerator(config.get(intStart), config.get(intEnd)),
                intStart,
                intEnd);
    }

    @Override
    public DataGeneratorContainer visit(DecimalType decimalType) {
        return DataGeneratorContainer.of(
                SequenceGeneratorFunction.bigDecimalGenerator(
                        config.get(intStart),
                        config.get(intEnd),
                        decimalType.getPrecision(),
                        decimalType.getScale()),
                intStart,
                intEnd);
    }
}
