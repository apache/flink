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

package org.apache.flink.table.data;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.types.AbstractDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.DataTypeFactoryMock;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.InstantiationUtil;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

import static java.util.Arrays.asList;
import static org.apache.flink.table.api.DataTypes.ARRAY;
import static org.apache.flink.table.api.DataTypes.BINARY;
import static org.apache.flink.table.api.DataTypes.BOOLEAN;
import static org.apache.flink.table.api.DataTypes.CHAR;
import static org.apache.flink.table.api.DataTypes.DATE;
import static org.apache.flink.table.api.DataTypes.DAY;
import static org.apache.flink.table.api.DataTypes.DECIMAL;
import static org.apache.flink.table.api.DataTypes.DOUBLE;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.INTERVAL;
import static org.apache.flink.table.api.DataTypes.MAP;
import static org.apache.flink.table.api.DataTypes.MONTH;
import static org.apache.flink.table.api.DataTypes.MULTISET;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.SECOND;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.DataTypes.TIME;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP_WITH_TIME_ZONE;
import static org.apache.flink.table.api.DataTypes.VARBINARY;
import static org.apache.flink.table.api.DataTypes.VARCHAR;
import static org.apache.flink.table.api.DataTypes.YEAR;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertArrayEquals;

/** Tests for {@link DataStructureConverters}. */
@RunWith(Parameterized.class)
public class DataStructureConvertersTest {

    @Parameters(name = "{index}: {0}")
    public static List<TestSpec> testData() {
        // ordered by definition in DataStructureConverters
        return asList(
                TestSpec.forDataType(CHAR(5))
                        .convertedTo(String.class, "12345")
                        .convertedTo(byte[].class, "12345".getBytes(StandardCharsets.UTF_8))
                        .convertedTo(StringData.class, StringData.fromString("12345")),
                TestSpec.forDataType(VARCHAR(100))
                        .convertedTo(String.class, "12345")
                        .convertedTo(byte[].class, "12345".getBytes(StandardCharsets.UTF_8))
                        .convertedTo(StringData.class, StringData.fromString("12345")),
                TestSpec.forDataType(BOOLEAN().notNull())
                        .convertedTo(Boolean.class, true)
                        .convertedTo(boolean.class, true),
                TestSpec.forDataType(BINARY(5))
                        .convertedTo(byte[].class, new byte[] {1, 2, 3, 4, 5}),
                TestSpec.forDataType(VARBINARY(100))
                        .convertedTo(byte[].class, new byte[] {1, 2, 3, 4, 5}),
                TestSpec.forDataType(DECIMAL(3, 2))
                        .convertedTo(BigDecimal.class, new BigDecimal("1.23"))
                        .convertedTo(DecimalData.class, DecimalData.fromUnscaledLong(123, 3, 2)),

                // TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, DOUBLE are skipped for simplicity

                TestSpec.forDataType(DATE())
                        .convertedTo(Date.class, Date.valueOf("2010-11-12"))
                        .convertedTo(LocalDate.class, LocalDate.parse("2010-11-12"))
                        .convertedTo(Integer.class, 14_925),
                TestSpec.forDataType(TIME(0))
                        .convertedTo(java.sql.Time.class, java.sql.Time.valueOf("12:34:56"))
                        .convertedTo(LocalTime.class, LocalTime.parse("12:34:56"))
                        .convertedTo(Integer.class, 45_296_000)
                        .convertedTo(Long.class, 45_296_000_000_000L),
                TestSpec.forDataType(TIME(3)) // TODO support precision of 9
                        .convertedTo(LocalTime.class, LocalTime.parse("12:34:56.001"))
                        .convertedTo(Integer.class, 45_296_001),
                TestSpec.forDataType(TIMESTAMP(9))
                        .convertedTo(
                                Timestamp.class, Timestamp.valueOf("2010-11-12 12:34:56.000000001"))
                        .convertedTo(
                                LocalDateTime.class,
                                LocalDateTime.parse("2010-11-12T12:34:56.000000001"))
                        .convertedTo(
                                TimestampData.class,
                                TimestampData.fromEpochMillis(1_289_565_296_000L, 1)),
                TestSpec.forDataType(TIMESTAMP_WITH_TIME_ZONE(0))
                        .convertedTo(
                                ZonedDateTime.class,
                                ZonedDateTime.ofInstant(Instant.EPOCH, ZoneId.of("UTC")))
                        .convertedTo(
                                java.time.OffsetDateTime.class,
                                ZonedDateTime.ofInstant(Instant.EPOCH, ZoneId.of("UTC"))
                                        .toOffsetDateTime())
                        .expectErrorMessage("Unsupported data type: TIMESTAMP(0) WITH TIME ZONE"),
                TestSpec.forDataType(TIMESTAMP_WITH_LOCAL_TIME_ZONE(0))
                        .convertedTo(Instant.class, Instant.ofEpochSecond(12_345))
                        .convertedTo(Integer.class, 12_345)
                        .convertedTo(Long.class, 12_345_000L)
                        .convertedTo(
                                TimestampData.class, TimestampData.fromEpochMillis(12_345_000)),
                TestSpec.forDataType(TIMESTAMP_WITH_LOCAL_TIME_ZONE(3))
                        .convertedTo(Instant.class, Instant.ofEpochSecond(12_345, 1_000_000))
                        .convertedTo(Long.class, 12_345_001L)
                        .convertedTo(
                                TimestampData.class, TimestampData.fromEpochMillis(12_345_001)),
                TestSpec.forDataType(TIMESTAMP_WITH_LOCAL_TIME_ZONE(9))
                        .convertedTo(Instant.class, Instant.ofEpochSecond(12_345, 1))
                        .convertedTo(
                                TimestampData.class, TimestampData.fromEpochMillis(12_345_000, 1)),
                TestSpec.forDataType(INTERVAL(YEAR(2), MONTH()))
                        .convertedTo(Period.class, Period.of(2, 6, 0))
                        .convertedTo(Integer.class, 30),
                TestSpec.forDataType(INTERVAL(MONTH()))
                        .convertedTo(Period.class, Period.of(0, 30, 0))
                        .convertedTo(Integer.class, 30),
                TestSpec.forDataType(INTERVAL(DAY(), SECOND(3))) // TODO support precision of 9
                        .convertedTo(Duration.class, Duration.ofMillis(123))
                        .convertedTo(Long.class, 123L),
                TestSpec.forDataType(ARRAY(BOOLEAN().notNull()))
                        .convertedTo(boolean[].class, new boolean[] {true, false, true, true})
                        .convertedTo(
                                ArrayData.class,
                                new GenericArrayData(new boolean[] {true, false, true, true})),
                TestSpec.forDataType(ARRAY(BOOLEAN()))
                        .convertedTo(Boolean[].class, new Boolean[] {true, null, true, true})
                        .convertedTo(List.class, Arrays.asList(true, null, true, true))
                        .convertedTo(
                                ArrayData.class,
                                new GenericArrayData(new Boolean[] {true, null, true, true})),
                TestSpec.forDataType(
                                ARRAY(INT().notNull().bridgedTo(int.class))) // int.class should not
                        // have an impact
                        .convertedTo(int[].class, new int[] {1, 2, 3, 4})
                        .convertedTo(Integer[].class, new Integer[] {1, 2, 3, 4})
                        .convertedTo(
                                List.class,
                                new LinkedList<>(
                                        Arrays.asList(
                                                1, 2, 3,
                                                4))), // test List that is not backed by an array

                // arrays of TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, DOUBLE are skipped for
                // simplicity

                TestSpec.forDataType(ARRAY(DATE()))
                        .convertedTo(
                                LocalDate[].class,
                                new LocalDate[] {
                                    null,
                                    LocalDate.parse("2010-11-12"),
                                    null,
                                    LocalDate.parse("2010-11-12")
                                })
                        .convertedTo(
                                List.class,
                                Arrays.asList(
                                        null,
                                        LocalDate.parse("2010-11-12"),
                                        null,
                                        LocalDate.parse("2010-11-12"))),
                TestSpec.forDataType(
                                MAP(
                                        INT().bridgedTo(int.class),
                                        BOOLEAN())) // int.class should not have an impact
                        .convertedTo(Map.class, createIdentityMap())
                        .convertedTo(MapData.class, new GenericMapData(createIdentityMap())),
                TestSpec.forDataType(MAP(DATE(), BOOLEAN()))
                        .convertedTo(Map.class, createLocalDateMap()),
                TestSpec.forDataType(MULTISET(BOOLEAN()))
                        .convertedTo(Map.class, createIdentityMultiset())
                        .convertedTo(MapData.class, new GenericMapData(createIdentityMultiset())),
                TestSpec.forDataType(MULTISET(DATE()))
                        .convertedTo(Map.class, createLocalDateMultiset()),
                TestSpec.forDataType(
                                ROW(
                                        FIELD("a", INT()),
                                        FIELD(
                                                "b",
                                                ROW(
                                                        FIELD("b_1", DOUBLE()),
                                                        FIELD("b_2", BOOLEAN())))))
                        .convertedTo(Row.class, Row.ofKind(RowKind.DELETE, 12, Row.of(2.0, null)))
                        .convertedToSupplier(
                                Row.class,
                                () -> {
                                    final Row namedRow = Row.withNames(RowKind.DELETE);
                                    namedRow.setField("a", 12);
                                    final Row sparseNamedRow = Row.withNames();
                                    sparseNamedRow.setField("b_1", 2.0); // "b_2" is omitted
                                    namedRow.setField("b", sparseNamedRow);
                                    return namedRow;
                                })
                        .convertedTo(
                                RowData.class,
                                GenericRowData.ofKind(
                                        RowKind.DELETE, 12, GenericRowData.of(2.0, null))),
                TestSpec.forDataType(
                                ROW(
                                        FIELD("a", INT()),
                                        FIELD(
                                                "b",
                                                ROW(FIELD("b_1", DATE()), FIELD("b_2", DATE())))))
                        .convertedTo(Row.class, Row.of(12, Row.of(LocalDate.ofEpochDay(1), null))),
                TestSpec.forClass(PojoWithMutableFields.class)
                        .convertedToSupplier(
                                PojoWithMutableFields.class,
                                () -> {
                                    final PojoWithMutableFields pojo = new PojoWithMutableFields();
                                    pojo.age = 42;
                                    pojo.name = "Bob";
                                    return pojo;
                                })
                        .convertedTo(Row.class, Row.of(42, "Bob"))
                        .convertedTo(
                                RowData.class, GenericRowData.of(42, StringData.fromString("Bob"))),
                TestSpec.forClass(PojoWithImmutableFields.class)
                        .convertedTo(
                                PojoWithImmutableFields.class,
                                new PojoWithImmutableFields(42, "Bob"))
                        .convertedTo(Row.class, Row.of(42, "Bob"))
                        .convertedTo(
                                RowData.class, GenericRowData.of(42, StringData.fromString("Bob"))),
                TestSpec.forClass(PojoWithGettersAndSetters.class)
                        .convertedToSupplier(
                                PojoWithGettersAndSetters.class,
                                () -> {
                                    final PojoWithGettersAndSetters pojo =
                                            new PojoWithGettersAndSetters();
                                    pojo.setAge(42);
                                    pojo.setName("Bob");
                                    return pojo;
                                })
                        .convertedTo(Row.class, Row.of(42, "Bob"))
                        .convertedTo(
                                RowData.class, GenericRowData.of(42, StringData.fromString("Bob"))),
                TestSpec.forClass(ComplexPojo.class)
                        .convertedToSupplier(
                                ComplexPojo.class,
                                () -> {
                                    final ComplexPojo pojo = new ComplexPojo();
                                    pojo.setTimestamp(
                                            Timestamp.valueOf("2010-11-12 13:14:15.000000001"));
                                    pojo.setPreferences(
                                            Row.of(42, "Bob", new Boolean[] {true, null, false}));
                                    pojo.setBalance(new BigDecimal("1.23"));
                                    return pojo;
                                })
                        .convertedTo(
                                Row.class,
                                Row.of(
                                        Timestamp.valueOf("2010-11-12 13:14:15.000000001"),
                                        Row.of(42, "Bob", new Boolean[] {true, null, false}),
                                        new BigDecimal("1.23"))),
                TestSpec.forClass(PojoAsSuperclass.class)
                        .convertedToSupplier(
                                PojoWithMutableFields.class,
                                () -> {
                                    final PojoWithMutableFields pojo = new PojoWithMutableFields();
                                    pojo.age = 42;
                                    pojo.name = "Bob";
                                    return pojo;
                                })
                        .convertedTo(Row.class, Row.of(42)),
                TestSpec.forDataType(MAP(STRING(), DataTypes.of(PojoWithImmutableFields.class)))
                        .convertedTo(Map.class, createPojoWithImmutableFieldsMap()),
                TestSpec.forDataType(ARRAY(DataTypes.of(PojoWithNestedPojo.class)))
                        .convertedTo(PojoWithNestedPojo[].class, createPojoWithNestedPojoArray())
                        .convertedTo(
                                Row[].class,
                                new Row[] {
                                    Row.of(
                                            new PojoWithImmutableFields(42, "Bob"),
                                            new PojoWithImmutableFields[] {
                                                new PojoWithImmutableFields(42, "Bob"), null
                                            }),
                                    null,
                                    Row.of(null, new PojoWithImmutableFields[3]),
                                    Row.of(null, null)
                                })
                        .convertedToWithAnotherValue(
                                Row[].class,
                                new Row[] {
                                    Row.of(null, null),
                                    Row.of(new PojoWithImmutableFields(10, "Bob"), null)
                                }),
                TestSpec.forDataType(DataTypes.of(PojoWithList.class))
                        .convertedTo(
                                PojoWithList.class,
                                new PojoWithList(
                                        Arrays.asList(
                                                Arrays.asList(1.0, null, 2.0, null),
                                                Collections.emptyList(),
                                                null)))
                        .convertedTo(
                                Row.class,
                                Row.of(
                                        Arrays.asList(
                                                Arrays.asList(1.0, null, 2.0, null),
                                                Collections.emptyList(),
                                                null))));
    }

    @Parameter public TestSpec testSpec;

    @Rule public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testConversions() {
        if (testSpec.expectedErrorMessage != null) {
            thrown.expect(TableException.class);
            thrown.expectMessage(equalTo(testSpec.expectedErrorMessage));
        }
        for (Map.Entry<Class<?>, Object> from : testSpec.conversions.entrySet()) {
            final DataType fromDataType = testSpec.dataType.bridgedTo(from.getKey());

            final DataStructureConverter<Object, Object> fromConverter =
                    simulateSerialization(DataStructureConverters.getConverter(fromDataType));
            fromConverter.open(DataStructureConvertersTest.class.getClassLoader());

            final Object internalValue = fromConverter.toInternalOrNull(from.getValue());

            final Object anotherValue = testSpec.conversionsWithAnotherValue.get(from.getKey());
            if (anotherValue != null) {
                fromConverter.toInternalOrNull(anotherValue);
            }

            for (Map.Entry<Class<?>, Object> to : testSpec.conversions.entrySet()) {
                final DataType toDataType = testSpec.dataType.bridgedTo(to.getKey());

                final DataStructureConverter<Object, Object> toConverter =
                        simulateSerialization(DataStructureConverters.getConverter(toDataType));
                toConverter.open(DataStructureConvertersTest.class.getClassLoader());

                assertArrayEquals(
                        new Object[] {to.getValue()},
                        new Object[] {toConverter.toExternalOrNull(internalValue)});
            }
        }
    }

    // --------------------------------------------------------------------------------------------
    // Test utilities
    // --------------------------------------------------------------------------------------------

    private static class TestSpec {

        private final String description;

        private final DataType dataType;

        private final Map<Class<?>, Object> conversions;

        private final Map<Class<?>, Object> conversionsWithAnotherValue;

        private @Nullable String expectedErrorMessage;

        private TestSpec(String description, DataType dataType) {
            this.description = description;
            this.dataType = dataType;
            this.conversions = new LinkedHashMap<>();
            this.conversionsWithAnotherValue = new LinkedHashMap<>();
        }

        static TestSpec forDataType(AbstractDataType<?> dataType) {
            final DataTypeFactoryMock factoryMock = new DataTypeFactoryMock();
            final DataType resolvedDataType = factoryMock.createDataType(dataType);
            return new TestSpec(resolvedDataType.toString(), resolvedDataType);
        }

        static TestSpec forClass(Class<?> clazz) {
            return forDataType(DataTypes.of(clazz));
        }

        <T> TestSpec convertedTo(Class<T> clazz, T value) {
            conversions.put(clazz, value);
            return this;
        }

        <T> TestSpec convertedToWithAnotherValue(Class<T> clazz, T value) {
            conversionsWithAnotherValue.put(clazz, value);
            return this;
        }

        <T> TestSpec convertedToSupplier(Class<T> clazz, Supplier<T> supplier) {
            conversions.put(clazz, supplier.get());
            return this;
        }

        TestSpec expectErrorMessage(String expectedErrorMessage) {
            this.expectedErrorMessage = expectedErrorMessage;
            return this;
        }

        @Override
        public String toString() {
            return description;
        }
    }

    private static DataStructureConverter<Object, Object> simulateSerialization(
            DataStructureConverter<Object, Object> converter) {
        try {
            final byte[] bytes = InstantiationUtil.serializeObject(converter);
            return InstantiationUtil.deserializeObject(
                    bytes, DataStructureConverter.class.getClassLoader());
        } catch (Exception e) {
            throw new AssertionError("Serialization failed.", e);
        }
    }

    private static Map<Integer, Boolean> createIdentityMap() {
        final Map<Integer, Boolean> map = new HashMap<>();
        map.put(1, true);
        map.put(2, false);
        map.put(3, null);
        map.put(null, true);
        return map;
    }

    private static Map<LocalDate, Boolean> createLocalDateMap() {
        final Map<LocalDate, Boolean> map = new HashMap<>();
        map.put(LocalDate.ofEpochDay(0), true);
        map.put(LocalDate.ofEpochDay(1), false);
        map.put(LocalDate.ofEpochDay(3), null);
        map.put(null, true);
        return map;
    }

    private static Map<String, PojoWithImmutableFields> createPojoWithImmutableFieldsMap() {
        final Map<String, PojoWithImmutableFields> map = new HashMap<>();
        map.put("Alice", new PojoWithImmutableFields(12, "Alice"));
        map.put("Bob", new PojoWithImmutableFields(42, "Bob"));
        map.put("Unknown", null);
        return map;
    }

    private static PojoWithNestedPojo[] createPojoWithNestedPojoArray() {
        final PojoWithNestedPojo pojo1 = new PojoWithNestedPojo();
        pojo1.inner = new PojoWithImmutableFields(42, "Bob");
        pojo1.innerArray =
                new PojoWithImmutableFields[] {new PojoWithImmutableFields(42, "Bob"), null};

        final PojoWithNestedPojo pojo2 = new PojoWithNestedPojo();
        pojo2.inner = null;
        pojo2.innerArray = new PojoWithImmutableFields[3];

        final PojoWithNestedPojo pojo3 = new PojoWithNestedPojo();

        return new PojoWithNestedPojo[] {pojo1, null, pojo2, pojo3};
    }

    private static Map<Boolean, Integer> createIdentityMultiset() {
        final Map<Boolean, Integer> map = new HashMap<>();
        map.put(true, 1);
        map.put(false, 2);
        map.put(null, 3);
        return map;
    }

    private static Map<LocalDate, Integer> createLocalDateMultiset() {
        final Map<LocalDate, Integer> map = new HashMap<>();
        map.put(LocalDate.ofEpochDay(0), 1);
        map.put(LocalDate.ofEpochDay(1), 2);
        map.put(null, 3);
        return map;
    }

    // --------------------------------------------------------------------------------------------
    // Structured types
    // --------------------------------------------------------------------------------------------

    /** POJO as superclass. */
    public static class PojoAsSuperclass {
        public int age;

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof PojoAsSuperclass)) {
                return false;
            }
            PojoAsSuperclass that = (PojoAsSuperclass) o;
            return age == that.age;
        }

        @Override
        public int hashCode() {
            return Objects.hash(age);
        }
    }

    /** POJO with public mutable fields. */
    public static class PojoWithMutableFields extends PojoAsSuperclass {
        public String name;

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null) {
                return false;
            }
            if (!super.equals(o)) {
                return false;
            }
            // modified to support PojoAsSuperclass
            if (o.getClass() == PojoAsSuperclass.class) {
                return true;
            }
            PojoWithMutableFields that = (PojoWithMutableFields) o;
            return Objects.equals(name, that.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), name);
        }
    }

    /** POJO with immutable fields. */
    public static class PojoWithImmutableFields {
        public final int age;
        public final String name;

        public PojoWithImmutableFields(int age, String name) {
            this.age = age;
            this.name = name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PojoWithImmutableFields that = (PojoWithImmutableFields) o;
            return age == that.age && Objects.equals(name, that.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(age, name);
        }
    }

    /** POJO with default constructor and private fields. */
    public static class PojoWithGettersAndSetters {
        private int age;
        private String name;

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PojoWithGettersAndSetters that = (PojoWithGettersAndSetters) o;
            return age == that.age && Objects.equals(name, that.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(age, name);
        }
    }

    /** POJO with annotations, nested types, and custom field order. */
    public static class ComplexPojo {
        private Timestamp timestamp;
        private @DataTypeHint("ROW<age INT, name STRING, mask ARRAY<BOOLEAN>>") Row preferences;
        private @DataTypeHint("DECIMAL(3, 2)") BigDecimal balance;

        public ComplexPojo() {
            // default constructor
        }

        // determines the order of the fields
        public ComplexPojo(Timestamp timestamp, Row preferences, BigDecimal balance) {
            this.timestamp = timestamp;
            this.preferences = preferences;
            this.balance = balance;
        }

        public Timestamp getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(Timestamp timestamp) {
            this.timestamp = timestamp;
        }

        public Row getPreferences() {
            return preferences;
        }

        public void setPreferences(Row preferences) {
            this.preferences = preferences;
        }

        public BigDecimal getBalance() {
            return balance;
        }

        public void setBalance(BigDecimal balance) {
            this.balance = balance;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ComplexPojo that = (ComplexPojo) o;
            return Objects.equals(timestamp, that.timestamp)
                    && Objects.equals(preferences, that.preferences)
                    && Objects.equals(balance, that.balance);
        }

        @Override
        public int hashCode() {
            return Objects.hash(timestamp, preferences, balance);
        }
    }

    /** POJO with nested fields. */
    public static class PojoWithNestedPojo {
        public PojoWithImmutableFields inner;

        public PojoWithImmutableFields[] innerArray;

        public PojoWithNestedPojo() {
            // default constructor
        }

        public PojoWithNestedPojo(
                PojoWithImmutableFields inner, PojoWithImmutableFields[] innerArray) {
            this.inner = inner;
            this.innerArray = innerArray;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PojoWithNestedPojo that = (PojoWithNestedPojo) o;
            return Objects.equals(inner, that.inner) && Arrays.equals(innerArray, that.innerArray);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(inner);
            result = 31 * result + Arrays.hashCode(innerArray);
            return result;
        }
    }

    /** Pojo with {@link List}. */
    public static class PojoWithList {

        public List<List<Double>> factors;

        public PojoWithList(List<List<Double>> factors) {
            this.factors = factors;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PojoWithList that = (PojoWithList) o;
            return Objects.equals(factors, that.factors);
        }

        @Override
        public int hashCode() {
            return Objects.hash(factors);
        }
    }
}
