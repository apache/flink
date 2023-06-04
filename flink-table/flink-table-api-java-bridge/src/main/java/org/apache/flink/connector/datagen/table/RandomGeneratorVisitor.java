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
import org.apache.flink.connector.datagen.table.types.DataGeneratorMapper;
import org.apache.flink.connector.datagen.table.types.DecimalDataRandomGenerator;
import org.apache.flink.connector.datagen.table.types.RowDataGenerator;
import org.apache.flink.streaming.api.functions.source.datagen.DataGenerator;
import org.apache.flink.streaming.api.functions.source.datagen.RandomGenerator;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DayTimeIntervalType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.YearMonthIntervalType;
import org.apache.flink.table.types.logical.ZonedTimestampType;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static org.apache.flink.configuration.ConfigOptions.key;

/** Creates a random {@link DataGeneratorContainer} for a particular logical type. */
@Internal
@SuppressWarnings("unchecked")
public class RandomGeneratorVisitor extends DataGenVisitorBase {

    public static final int RANDOM_STRING_LENGTH_DEFAULT = 100;

    public static final int RANDOM_BYTES_LENGTH_DEFAULT = 100;

    private static final int RANDOM_COLLECTION_LENGTH_DEFAULT = 3;

    private static final float NULL_RATE_DEFAULT = 0f;

    private final ConfigOptions.OptionBuilder minKey;

    private final ConfigOptions.OptionBuilder maxKey;

    private final ConfigOptions.OptionBuilder maxPastKey;

    private final ConfigOptions.OptionBuilder nullRate;

    public RandomGeneratorVisitor(String name, ReadableConfig config) {
        super(name, config);

        this.minKey =
                key(
                        DataGenConnectorOptionsUtil.FIELDS
                                + "."
                                + name
                                + "."
                                + DataGenConnectorOptionsUtil.MIN);
        this.maxKey =
                key(
                        DataGenConnectorOptionsUtil.FIELDS
                                + "."
                                + name
                                + "."
                                + DataGenConnectorOptionsUtil.MAX);
        this.maxPastKey =
                key(
                        DataGenConnectorOptionsUtil.FIELDS
                                + "."
                                + name
                                + "."
                                + DataGenConnectorOptionsUtil.MAX_PAST);

        this.nullRate =
                key(
                        DataGenConnectorOptionsUtil.FIELDS
                                + "."
                                + name
                                + "."
                                + DataGenConnectorOptionsUtil.NULL_RATE);
    }

    @Override
    public DataGeneratorContainer visit(BooleanType booleanType) {
        ConfigOption<Float> nr = nullRate.floatType().defaultValue(NULL_RATE_DEFAULT);
        return DataGeneratorContainer.of(
                RandomGenerator.booleanGenerator().withNullRate(config.get(nr)), nr);
    }

    @Override
    public DataGeneratorContainer visit(CharType charType) {
        ConfigOption<Integer> lenOption =
                key(DataGenConnectorOptionsUtil.FIELDS
                                + "."
                                + name
                                + "."
                                + DataGenConnectorOptionsUtil.LENGTH)
                        .intType()
                        .defaultValue(RANDOM_STRING_LENGTH_DEFAULT);
        ConfigOption<Float> nr = nullRate.floatType().defaultValue(NULL_RATE_DEFAULT);
        return DataGeneratorContainer.of(
                getRandomStringGenerator(config.get(lenOption)).withNullRate(config.get(nr)),
                lenOption,
                nr);
    }

    @Override
    public DataGeneratorContainer visit(VarCharType varCharType) {
        ConfigOption<Integer> lenOption =
                key(DataGenConnectorOptionsUtil.FIELDS
                                + "."
                                + name
                                + "."
                                + DataGenConnectorOptionsUtil.LENGTH)
                        .intType()
                        .defaultValue(RANDOM_STRING_LENGTH_DEFAULT);
        ConfigOption<Float> nr = nullRate.floatType().defaultValue(NULL_RATE_DEFAULT);
        return DataGeneratorContainer.of(
                getRandomStringGenerator(config.get(lenOption)).withNullRate(config.get(nr)),
                lenOption,
                nr);
    }

    @Override
    public DataGeneratorContainer visit(BinaryType binaryType) {
        ConfigOption<Integer> lenOption =
                key(DataGenConnectorOptionsUtil.FIELDS
                                + "."
                                + name
                                + "."
                                + DataGenConnectorOptionsUtil.LENGTH)
                        .intType()
                        .defaultValue(RANDOM_BYTES_LENGTH_DEFAULT);
        return DataGeneratorContainer.of(getRandomBytesGenerator(config.get(lenOption)), lenOption);
    }

    @Override
    public DataGeneratorContainer visit(VarBinaryType varBinaryType) {
        ConfigOption<Integer> lenOption =
                key(DataGenConnectorOptionsUtil.FIELDS
                                + "."
                                + name
                                + "."
                                + DataGenConnectorOptionsUtil.LENGTH)
                        .intType()
                        .defaultValue(RANDOM_BYTES_LENGTH_DEFAULT);
        return DataGeneratorContainer.of(getRandomBytesGenerator(config.get(lenOption)), lenOption);
    }

    @Override
    public DataGeneratorContainer visit(TinyIntType tinyIntType) {
        ConfigOption<Integer> min = minKey.intType().defaultValue((int) Byte.MIN_VALUE);
        ConfigOption<Integer> max = maxKey.intType().defaultValue((int) Byte.MAX_VALUE);
        ConfigOption<Float> nr = nullRate.floatType().defaultValue(NULL_RATE_DEFAULT);
        return DataGeneratorContainer.of(
                RandomGenerator.byteGenerator(
                                config.get(min).byteValue(), config.get(max).byteValue())
                        .withNullRate(config.get(nr)),
                min,
                max,
                nr);
    }

    @Override
    public DataGeneratorContainer visit(SmallIntType smallIntType) {
        ConfigOption<Integer> min = minKey.intType().defaultValue((int) Short.MIN_VALUE);
        ConfigOption<Integer> max = maxKey.intType().defaultValue((int) Short.MAX_VALUE);
        ConfigOption<Float> nr = nullRate.floatType().defaultValue(NULL_RATE_DEFAULT);
        return DataGeneratorContainer.of(
                RandomGenerator.shortGenerator(
                                config.get(min).shortValue(), config.get(max).shortValue())
                        .withNullRate(config.get(nr)),
                min,
                max,
                nr);
    }

    @Override
    public DataGeneratorContainer visit(IntType integerType) {
        ConfigOption<Integer> min = minKey.intType().defaultValue(Integer.MIN_VALUE);
        ConfigOption<Integer> max = maxKey.intType().defaultValue(Integer.MAX_VALUE);
        ConfigOption<Float> nr = nullRate.floatType().defaultValue(NULL_RATE_DEFAULT);
        return DataGeneratorContainer.of(
                RandomGenerator.intGenerator(config.get(min), config.get(max))
                        .withNullRate(config.get(nr)),
                min,
                max,
                nr);
    }

    @Override
    public DataGeneratorContainer visit(BigIntType bigIntType) {
        ConfigOption<Long> min = minKey.longType().defaultValue(Long.MIN_VALUE);
        ConfigOption<Long> max = maxKey.longType().defaultValue(Long.MAX_VALUE);
        ConfigOption<Float> nr = nullRate.floatType().defaultValue(NULL_RATE_DEFAULT);
        return DataGeneratorContainer.of(
                RandomGenerator.longGenerator(config.get(min), config.get(max))
                        .withNullRate(config.get(nr)),
                min,
                max,
                nr);
    }

    @Override
    public DataGeneratorContainer visit(FloatType floatType) {
        ConfigOption<Float> min = minKey.floatType().defaultValue(Float.MIN_VALUE);
        ConfigOption<Float> max = maxKey.floatType().defaultValue(Float.MAX_VALUE);
        ConfigOption<Float> nr = nullRate.floatType().defaultValue(NULL_RATE_DEFAULT);
        return DataGeneratorContainer.of(
                RandomGenerator.floatGenerator(config.get(min), config.get(max))
                        .withNullRate(config.get(nr)),
                min,
                max,
                nr);
    }

    @Override
    public DataGeneratorContainer visit(DoubleType doubleType) {
        ConfigOption<Double> min = minKey.doubleType().defaultValue(Double.MIN_VALUE);
        ConfigOption<Double> max = maxKey.doubleType().defaultValue(Double.MAX_VALUE);
        ConfigOption<Float> nr = nullRate.floatType().defaultValue(NULL_RATE_DEFAULT);
        return DataGeneratorContainer.of(
                RandomGenerator.doubleGenerator(config.get(min), config.get(max))
                        .withNullRate(config.get(nr)),
                min,
                max,
                nr);
    }

    @Override
    public DataGeneratorContainer visit(DecimalType decimalType) {
        ConfigOption<Double> min = minKey.doubleType().defaultValue(Double.MIN_VALUE);
        ConfigOption<Double> max = maxKey.doubleType().defaultValue(Double.MAX_VALUE);
        ConfigOption<Float> nr = nullRate.floatType().defaultValue(NULL_RATE_DEFAULT);
        return DataGeneratorContainer.of(
                new DecimalDataRandomGenerator(
                        decimalType.getPrecision(),
                        decimalType.getScale(),
                        config.get(min),
                        config.get(max),
                        config.get(nr)),
                min,
                max,
                nr);
    }

    @Override
    public DataGeneratorContainer visit(YearMonthIntervalType yearMonthIntervalType) {
        ConfigOption<Integer> min = minKey.intType().defaultValue(0);
        ConfigOption<Integer> max = maxKey.intType().defaultValue(120000); // Period max
        ConfigOption<Float> nr = nullRate.floatType().defaultValue(NULL_RATE_DEFAULT);
        return DataGeneratorContainer.of(
                RandomGenerator.intGenerator(config.get(min), config.get(max))
                        .withNullRate(config.get(nr)),
                min,
                max,
                nr);
    }

    @Override
    public DataGeneratorContainer visit(DayTimeIntervalType dayTimeIntervalType) {
        ConfigOption<Long> min = minKey.longType().defaultValue(Long.MIN_VALUE);
        ConfigOption<Long> max = maxKey.longType().defaultValue(Long.MAX_VALUE);
        ConfigOption<Float> nr = nullRate.floatType().defaultValue(NULL_RATE_DEFAULT);
        return DataGeneratorContainer.of(
                RandomGenerator.longGenerator(config.get(min), config.get(max))
                        .withNullRate(config.get(nr)),
                min,
                max,
                nr);
    }

    @Override
    public DataGeneratorContainer visit(TimestampType timestampType) {
        ConfigOption<Duration> maxPastOption =
                maxPastKey.durationType().defaultValue(Duration.ZERO);
        ConfigOption<Float> nr = nullRate.floatType().defaultValue(NULL_RATE_DEFAULT);
        return DataGeneratorContainer.of(
                getRandomPastTimestampGenerator(config.get(maxPastOption))
                        .withNullRate(config.get(nr)),
                maxPastOption,
                nr);
    }

    @Override
    public DataGeneratorContainer visit(ZonedTimestampType zonedTimestampType) {
        ConfigOption<Duration> maxPastOption =
                maxPastKey.durationType().defaultValue(Duration.ZERO);
        ConfigOption<Float> nr = nullRate.floatType().defaultValue(NULL_RATE_DEFAULT);
        return DataGeneratorContainer.of(
                getRandomPastTimestampGenerator(config.get(maxPastOption))
                        .withNullRate(config.get(nr)),
                maxPastOption,
                nr);
    }

    @Override
    public DataGeneratorContainer visit(LocalZonedTimestampType localZonedTimestampType) {
        ConfigOption<Duration> maxPastOption =
                maxPastKey.durationType().defaultValue(Duration.ZERO);
        ConfigOption<Float> nr = nullRate.floatType().defaultValue(NULL_RATE_DEFAULT);
        return DataGeneratorContainer.of(
                getRandomPastTimestampGenerator(config.get(maxPastOption))
                        .withNullRate(config.get(nr)),
                maxPastOption,
                nr);
    }

    @Override
    public DataGeneratorContainer visit(ArrayType arrayType) {
        ConfigOption<Integer> lenOption =
                key(DataGenConnectorOptionsUtil.FIELDS
                                + "."
                                + name
                                + "."
                                + DataGenConnectorOptionsUtil.LENGTH)
                        .intType()
                        .defaultValue(RANDOM_COLLECTION_LENGTH_DEFAULT);

        String fieldName = name + "." + "element";
        DataGeneratorContainer container =
                arrayType.getElementType().accept(new RandomGeneratorVisitor(fieldName, config));
        ConfigOption<Float> nr = nullRate.floatType().defaultValue(NULL_RATE_DEFAULT);
        DataGenerator<Object[]> generator =
                RandomGenerator.arrayGenerator(container.getGenerator(), config.get(lenOption));
        Set<ConfigOption<?>> options = container.getOptions();
        options.add(nr);
        return DataGeneratorContainer.of(
                new DataGeneratorMapper<>(generator, (GenericArrayData::new), config.get(nr)),
                options.toArray(new ConfigOption<?>[0]));
    }

    @Override
    public DataGeneratorContainer visit(MultisetType multisetType) {
        ConfigOption<Integer> lenOption =
                key(DataGenConnectorOptionsUtil.FIELDS
                                + "."
                                + name
                                + "."
                                + DataGenConnectorOptionsUtil.LENGTH)
                        .intType()
                        .defaultValue(RANDOM_COLLECTION_LENGTH_DEFAULT);

        String fieldName = name + "." + "element";
        DataGeneratorContainer container =
                multisetType.getElementType().accept(new RandomGeneratorVisitor(fieldName, config));

        DataGenerator<Map<Object, Integer>> mapGenerator =
                RandomGenerator.mapGenerator(
                        container.getGenerator(),
                        RandomGenerator.intGenerator(0, 10),
                        config.get(lenOption));

        Set<ConfigOption<?>> options = container.getOptions();
        ConfigOption<Float> nr = nullRate.floatType().defaultValue(NULL_RATE_DEFAULT);
        options.add(nr);

        return DataGeneratorContainer.of(
                new DataGeneratorMapper<>(mapGenerator, GenericMapData::new, config.get(nr)),
                options.toArray(new ConfigOption<?>[0]));
    }

    @Override
    public DataGeneratorContainer visit(MapType mapType) {
        ConfigOption<Integer> lenOption =
                key(DataGenConnectorOptionsUtil.FIELDS
                                + "."
                                + name
                                + "."
                                + DataGenConnectorOptionsUtil.LENGTH)
                        .intType()
                        .defaultValue(RANDOM_COLLECTION_LENGTH_DEFAULT);

        String keyName = name + "." + "key";
        String valName = name + "." + "value";

        DataGeneratorContainer keyContainer =
                mapType.getKeyType().accept(new RandomGeneratorVisitor(keyName, config));

        DataGeneratorContainer valContainer =
                mapType.getValueType().accept(new RandomGeneratorVisitor(valName, config));

        ConfigOption<Float> nr = nullRate.floatType().defaultValue(NULL_RATE_DEFAULT);
        Set<ConfigOption<?>> options = keyContainer.getOptions();
        options.addAll(valContainer.getOptions());
        options.add(nr);

        DataGenerator<Map<Object, Object>> mapGenerator =
                RandomGenerator.mapGenerator(
                        keyContainer.getGenerator(),
                        valContainer.getGenerator(),
                        config.get(lenOption));

        return DataGeneratorContainer.of(
                new DataGeneratorMapper<>(mapGenerator, GenericMapData::new, config.get(nr)),
                options.toArray(new ConfigOption<?>[0]));
    }

    @Override
    public DataGeneratorContainer visit(RowType rowType) {
        List<DataGeneratorContainer> fieldContainers =
                rowType.getFields().stream()
                        .map(
                                field -> {
                                    String fieldName = name + "." + field.getName();
                                    return field.getType()
                                            .accept(new RandomGeneratorVisitor(fieldName, config));
                                })
                        .collect(Collectors.toList());

        List<ConfigOption<?>> fieldOptions =
                fieldContainers.stream()
                        .flatMap(container -> container.getOptions().stream())
                        .collect(Collectors.toList());

        ConfigOption<Float> nr = nullRate.floatType().defaultValue(NULL_RATE_DEFAULT);
        fieldOptions.add(nr);

        DataGenerator[] generators =
                fieldContainers.stream()
                        .map(DataGeneratorContainer::getGenerator)
                        .toArray(DataGenerator[]::new);

        return DataGeneratorContainer.of(
                new RowDataGenerator(generators, rowType.getFieldNames(), config.get(nr)),
                fieldOptions.toArray(new ConfigOption[0]));
    }

    @Override
    protected DataGeneratorContainer defaultMethod(LogicalType logicalType) {
        throw new ValidationException("Unsupported type: " + logicalType);
    }

    private static RandomGenerator<StringData> getRandomStringGenerator(int length) {
        return new RandomGenerator<StringData>() {
            @Override
            public StringData next() {
                if (nullRate == NULL_RATE_DEFAULT
                        || ThreadLocalRandom.current().nextFloat() > nullRate) {
                    return StringData.fromString(random.nextHexString(length));
                }
                return null;
            }
        };
    }

    private static RandomGenerator<TimestampData> getRandomPastTimestampGenerator(
            Duration maxPast) {
        return new RandomGenerator<TimestampData>() {
            @Override
            public TimestampData next() {
                if (nullRate == NULL_RATE_DEFAULT
                        || ThreadLocalRandom.current().nextFloat() > nullRate) {
                    long maxPastMillis = maxPast.toMillis();
                    long past = maxPastMillis > 0 ? random.nextLong(0, maxPastMillis) : 0;
                    return TimestampData.fromEpochMillis(System.currentTimeMillis() - past);
                }
                return null;
            }
        };
    }

    private static RandomGenerator<byte[]> getRandomBytesGenerator(int length) {
        return new RandomGenerator<byte[]>() {
            @Override
            public byte[] next() {
                byte[] arr = new byte[length];
                random.getRandomGenerator().nextBytes(arr);
                return arr;
            }
        };
    }
}
