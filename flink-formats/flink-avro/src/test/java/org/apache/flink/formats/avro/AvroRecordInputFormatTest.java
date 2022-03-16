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

package org.apache.flink.formats.avro;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.typeutils.runtime.kryo.Serializers;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.formats.avro.generated.Address;
import org.apache.flink.formats.avro.generated.Colors;
import org.apache.flink.formats.avro.generated.Fixed2;
import org.apache.flink.formats.avro.generated.User;
import org.apache.flink.formats.avro.typeutils.AvroTypeInfo;
import org.apache.flink.formats.avro.utils.AvroKryoSerializerUtils;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.util.Utf8;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.MapEntry.entry;

/**
 * Test the avro input format. (The testcase is mostly the getting started tutorial of avro)
 * http://avro.apache.org/docs/current/gettingstartedjava.html
 */
public class AvroRecordInputFormatTest {

    public File testFile;

    static final String TEST_NAME = "Alyssa";

    static final String TEST_ARRAY_STRING_1 = "ELEMENT 1";
    static final String TEST_ARRAY_STRING_2 = "ELEMENT 2";

    static final boolean TEST_ARRAY_BOOLEAN_1 = true;
    static final boolean TEST_ARRAY_BOOLEAN_2 = false;

    static final Colors TEST_ENUM_COLOR = Colors.GREEN;

    static final String TEST_MAP_KEY1 = "KEY 1";
    static final long TEST_MAP_VALUE1 = 8546456L;
    static final String TEST_MAP_KEY2 = "KEY 2";
    static final long TEST_MAP_VALUE2 = 17554L;

    static final int TEST_NUM = 239;
    static final String TEST_STREET = "Baker Street";
    static final String TEST_CITY = "London";
    static final String TEST_STATE = "London";
    static final String TEST_ZIP = "NW1 6XE";

    private final Schema userSchema = new User().getSchema();

    public static void writeTestFile(File testFile) throws IOException {
        ArrayList<CharSequence> stringArray = new ArrayList<>();
        stringArray.add(TEST_ARRAY_STRING_1);
        stringArray.add(TEST_ARRAY_STRING_2);

        ArrayList<Boolean> booleanArray = new ArrayList<>();
        booleanArray.add(TEST_ARRAY_BOOLEAN_1);
        booleanArray.add(TEST_ARRAY_BOOLEAN_2);

        HashMap<CharSequence, Long> longMap = new HashMap<>();
        longMap.put(TEST_MAP_KEY1, TEST_MAP_VALUE1);
        longMap.put(TEST_MAP_KEY2, TEST_MAP_VALUE2);

        Address addr = new Address();
        addr.setNum(TEST_NUM);
        addr.setStreet(TEST_STREET);
        addr.setCity(TEST_CITY);
        addr.setState(TEST_STATE);
        addr.setZip(TEST_ZIP);

        User user1 = new User();

        user1.setName(TEST_NAME);
        user1.setFavoriteNumber(256);
        user1.setTypeDoubleTest(123.45d);
        user1.setTypeBoolTest(true);
        user1.setTypeArrayString(stringArray);
        user1.setTypeArrayBoolean(booleanArray);
        user1.setTypeEnum(TEST_ENUM_COLOR);
        user1.setTypeMap(longMap);
        user1.setTypeNested(addr);
        user1.setTypeBytes(ByteBuffer.allocate(10));
        user1.setTypeDate(LocalDate.parse("2014-03-01"));
        user1.setTypeTimeMillis(LocalTime.parse("12:12:12"));
        user1.setTypeTimeMicros(LocalTime.ofSecondOfDay(0).plus(123456L, ChronoUnit.MICROS));
        user1.setTypeTimestampMillis(Instant.parse("2014-03-01T12:12:12.321Z"));
        user1.setTypeTimestampMicros(Instant.ofEpochSecond(0).plus(123456L, ChronoUnit.MICROS));
        // 20.00
        user1.setTypeDecimalBytes(
                ByteBuffer.wrap(BigDecimal.valueOf(2000, 2).unscaledValue().toByteArray()));
        // 20.00
        user1.setTypeDecimalFixed(
                new Fixed2(BigDecimal.valueOf(2000, 2).unscaledValue().toByteArray()));

        // Construct via builder
        User user2 =
                User.newBuilder()
                        .setName("Charlie")
                        .setFavoriteColor("blue")
                        .setFavoriteNumber(null)
                        .setTypeBoolTest(false)
                        .setTypeDoubleTest(1.337d)
                        .setTypeNullTest(null)
                        .setTypeLongTest(1337L)
                        .setTypeArrayString(new ArrayList<>())
                        .setTypeArrayBoolean(new ArrayList<>())
                        .setTypeNullableArray(null)
                        .setTypeEnum(Colors.RED)
                        .setTypeMap(new HashMap<>())
                        .setTypeFixed(null)
                        .setTypeUnion(null)
                        .setTypeNested(
                                Address.newBuilder()
                                        .setNum(TEST_NUM)
                                        .setStreet(TEST_STREET)
                                        .setCity(TEST_CITY)
                                        .setState(TEST_STATE)
                                        .setZip(TEST_ZIP)
                                        .build())
                        .setTypeBytes(ByteBuffer.allocate(10))
                        .setTypeDate(LocalDate.parse("2014-03-01"))
                        .setTypeTimeMillis(LocalTime.parse("12:12:12"))
                        .setTypeTimeMicros(
                                LocalTime.ofSecondOfDay(0).plus(123456L, ChronoUnit.MICROS))
                        .setTypeTimestampMillis(Instant.parse("2014-03-01T12:12:12.321Z"))
                        .setTypeTimestampMicros(
                                Instant.ofEpochSecond(0).plus(123456L, ChronoUnit.MICROS))
                        // 20.00
                        .setTypeDecimalBytes(
                                ByteBuffer.wrap(
                                        BigDecimal.valueOf(2000, 2).unscaledValue().toByteArray()))
                        // 20.00
                        .setTypeDecimalFixed(
                                new Fixed2(
                                        BigDecimal.valueOf(2000, 2).unscaledValue().toByteArray()))
                        .build();
        DatumWriter<User> userDatumWriter = new SpecificDatumWriter<>(User.class);
        DataFileWriter<User> dataFileWriter = new DataFileWriter<>(userDatumWriter);
        dataFileWriter.create(user1.getSchema(), testFile);
        dataFileWriter.append(user1);
        dataFileWriter.append(user2);
        dataFileWriter.close();
    }

    @BeforeEach
    public void createFiles() throws IOException {
        testFile = File.createTempFile("AvroInputFormatTest", null);
        writeTestFile(testFile);
    }

    /** Test if the AvroInputFormat is able to properly read data from an Avro file. */
    @Test
    void testDeserialization() throws IOException {
        Configuration parameters = new Configuration();

        AvroInputFormat<User> format =
                new AvroInputFormat<>(new Path(testFile.getAbsolutePath()), User.class);

        format.configure(parameters);
        FileInputSplit[] splits = format.createInputSplits(1);
        assertThat(splits).hasSize(1);
        format.open(splits[0]);

        User u = format.nextRecord(null);
        assertThat(u).isNotNull();

        String name = u.getName().toString();
        assertThat(name).isEqualTo(TEST_NAME);

        // check arrays
        List<CharSequence> sl = u.getTypeArrayString();
        assertThat(sl.get(0).toString()).isEqualTo(TEST_ARRAY_STRING_1);
        assertThat(sl.get(1).toString()).isEqualTo(TEST_ARRAY_STRING_2);

        List<Boolean> bl = u.getTypeArrayBoolean();
        assertThat(bl).containsExactly(TEST_ARRAY_BOOLEAN_1, TEST_ARRAY_BOOLEAN_2);

        // check enums
        Colors enumValue = u.getTypeEnum();
        assertThat(enumValue).isEqualTo(TEST_ENUM_COLOR);

        // check maps
        Map<CharSequence, Long> lm = u.getTypeMap();
        assertThat(lm)
                .containsOnly(
                        entry(new Utf8(TEST_MAP_KEY1), TEST_MAP_VALUE1),
                        entry(new Utf8(TEST_MAP_KEY2), TEST_MAP_VALUE2));

        assertThat(format.reachedEnd()).as("expecting second element").isFalse();
        assertThat(format.nextRecord(u)).as("expecting second element").isNotNull();

        assertThat(format.nextRecord(u)).isNull();
        assertThat(format.reachedEnd()).isTrue();

        format.close();
    }

    /** Test if the AvroInputFormat is able to properly read data from an Avro file. */
    @Test
    void testDeserializationReuseAvroRecordFalse() throws IOException {
        Configuration parameters = new Configuration();

        AvroInputFormat<User> format =
                new AvroInputFormat<>(new Path(testFile.getAbsolutePath()), User.class);
        format.setReuseAvroValue(false);

        format.configure(parameters);
        FileInputSplit[] splits = format.createInputSplits(1);
        assertThat(splits).hasSize(1);
        format.open(splits[0]);

        User u = format.nextRecord(null);
        assertThat(u).isNotNull();

        String name = u.getName().toString();
        assertThat(name).isEqualTo(TEST_NAME);

        // check arrays
        List<CharSequence> sl = u.getTypeArrayString();
        assertThat(sl.get(0).toString()).isEqualTo(TEST_ARRAY_STRING_1);
        assertThat(sl.get(1).toString()).isEqualTo(TEST_ARRAY_STRING_2);

        List<Boolean> bl = u.getTypeArrayBoolean();
        assertThat(bl).containsExactly(TEST_ARRAY_BOOLEAN_1, TEST_ARRAY_BOOLEAN_2);

        // check enums
        Colors enumValue = u.getTypeEnum();
        assertThat(enumValue).isEqualTo(TEST_ENUM_COLOR);

        // check maps
        Map<CharSequence, Long> lm = u.getTypeMap();
        assertThat(lm)
                .containsOnly(
                        entry(new Utf8(TEST_MAP_KEY1), TEST_MAP_VALUE1),
                        entry(new Utf8(TEST_MAP_KEY2), TEST_MAP_VALUE2));

        assertThat(format.reachedEnd()).as("expecting second element").isFalse();
        assertThat(format.nextRecord(u)).as("expecting second element").isNotNull();

        assertThat(format.nextRecord(u)).isNull();
        assertThat(format.reachedEnd()).isTrue();

        format.close();
    }

    /**
     * Test if the Flink serialization is able to properly process GenericData.Record types. Usually
     * users of Avro generate classes (POJOs) from Avro schemas. However, if generated classes are
     * not available, one can also use GenericData.Record. It is an untyped key-value record which
     * is using a schema to validate the correctness of the data.
     *
     * <p>It is not recommended to use GenericData.Record with Flink. Use generated POJOs instead.
     */
    @Test
    void testDeserializeToGenericType() throws IOException {
        DatumReader<GenericData.Record> datumReader = new GenericDatumReader<>(userSchema);

        try (FileReader<GenericData.Record> dataFileReader =
                DataFileReader.openReader(testFile, datumReader)) {
            // initialize Record by reading it from disk (that's easier than creating it by hand)
            GenericData.Record rec = new GenericData.Record(userSchema);
            dataFileReader.next(rec);

            // check if record has been read correctly
            assertThat(rec).isNotNull();
            assertThat(rec.get("name").toString()).isEqualTo(TEST_NAME);
            assertThat(rec.get("type_enum").toString()).isEqualTo(TEST_ENUM_COLOR.toString());
            assertThat(rec.get("type_long_test")).isNull(); // it is null for the first record.

            // now serialize it with our framework:
            TypeInformation<GenericData.Record> te =
                    TypeExtractor.createTypeInfo(GenericData.Record.class);

            ExecutionConfig ec = new ExecutionConfig();
            assertThat(te).isExactlyInstanceOf(GenericTypeInfo.class);

            Serializers.recursivelyRegisterType(te.getTypeClass(), ec, new HashSet<>());

            TypeSerializer<GenericData.Record> tser = te.createSerializer(ec);
            assertThat(ec.getDefaultKryoSerializerClasses())
                    .hasSize(1)
                    .containsEntry(
                            Schema.class, AvroKryoSerializerUtils.AvroSchemaSerializer.class);

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try (DataOutputViewStreamWrapper outView = new DataOutputViewStreamWrapper(out)) {
                tser.serialize(rec, outView);
            }

            GenericData.Record newRec;
            try (DataInputViewStreamWrapper inView =
                    new DataInputViewStreamWrapper(new ByteArrayInputStream(out.toByteArray()))) {
                newRec = tser.deserialize(inView);
            }

            // check if it is still the same
            assertThat(newRec).isNotNull();
            assertThat(newRec.get("name").toString()).isEqualTo(TEST_NAME);
            assertThat(newRec.get("type_enum").toString()).isEqualTo(TEST_ENUM_COLOR.toString());
            assertThat(newRec.get("type_long_test")).isNull();
        }
    }

    /** This test validates proper serialization with specific (generated POJO) types. */
    @Test
    void testDeserializeToSpecificType() throws IOException {

        DatumReader<User> datumReader = new SpecificDatumReader<>(userSchema);

        try (FileReader<User> dataFileReader = DataFileReader.openReader(testFile, datumReader)) {
            User rec = dataFileReader.next();

            // check if record has been read correctly
            assertThat(rec).isNotNull();
            assertThat(rec.get("name").toString()).isEqualTo(TEST_NAME);
            assertThat(rec.get("type_enum").toString()).isEqualTo(TEST_ENUM_COLOR.toString());

            // now serialize it with our framework:
            ExecutionConfig ec = new ExecutionConfig();
            TypeInformation<User> te = TypeExtractor.createTypeInfo(User.class);

            assertThat(te).isExactlyInstanceOf(AvroTypeInfo.class);
            TypeSerializer<User> tser = te.createSerializer(ec);

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try (DataOutputViewStreamWrapper outView = new DataOutputViewStreamWrapper(out)) {
                tser.serialize(rec, outView);
            }

            User newRec;
            try (DataInputViewStreamWrapper inView =
                    new DataInputViewStreamWrapper(new ByteArrayInputStream(out.toByteArray()))) {
                newRec = tser.deserialize(inView);
            }

            // check if it is still the same
            assertThat(newRec).isNotNull();
            assertThat(newRec.get("name").toString()).isEqualTo(TEST_NAME);
            assertThat(newRec.get("type_enum").toString()).isEqualTo(TEST_ENUM_COLOR.toString());
        }
    }

    /**
     * Test if the AvroInputFormat is able to properly read data from an Avro file as a
     * GenericRecord.
     */
    @Test
    void testDeserializationGenericRecord() throws IOException {
        Configuration parameters = new Configuration();

        AvroInputFormat<GenericRecord> format =
                new AvroInputFormat<>(new Path(testFile.getAbsolutePath()), GenericRecord.class);

        doTestDeserializationGenericRecord(format, parameters);
    }

    /**
     * Helper method to test GenericRecord serialisation.
     *
     * @param format the format to test
     * @param parameters the configuration to use
     * @throws IOException thrown id there is a issue
     */
    @SuppressWarnings("unchecked")
    private void doTestDeserializationGenericRecord(
            final AvroInputFormat<GenericRecord> format, final Configuration parameters)
            throws IOException {
        try {
            format.configure(parameters);
            FileInputSplit[] splits = format.createInputSplits(1);
            assertThat(splits).hasSize(1);
            format.open(splits[0]);

            GenericRecord u = format.nextRecord(null);
            assertThat(u).isNotNull();
            assertThat(u.getSchema()).isEqualTo(userSchema);

            String name = u.get("name").toString();
            assertThat(name).isEqualTo(TEST_NAME);

            // check arrays
            List<CharSequence> sl = (List<CharSequence>) u.get("type_array_string");
            assertThat(sl.get(0).toString()).isEqualTo(TEST_ARRAY_STRING_1);
            assertThat(sl.get(1).toString()).isEqualTo(TEST_ARRAY_STRING_2);

            List<Boolean> bl = (List<Boolean>) u.get("type_array_boolean");
            assertThat(bl).containsExactly(TEST_ARRAY_BOOLEAN_1, TEST_ARRAY_BOOLEAN_2);

            // check enums
            GenericData.EnumSymbol enumValue = (GenericData.EnumSymbol) u.get("type_enum");
            assertThat(enumValue).isEqualTo(TEST_ENUM_COLOR);

            // check maps
            Map<CharSequence, Long> lm = (Map<CharSequence, Long>) u.get("type_map");
            assertThat(lm)
                    .containsOnly(
                            entry(new Utf8(TEST_MAP_KEY1), TEST_MAP_VALUE1),
                            entry(new Utf8(TEST_MAP_KEY2), TEST_MAP_VALUE2));

            assertThat(format.reachedEnd()).as("expecting second element").isFalse();
            assertThat(format.nextRecord(u)).as("expecting second element").isNotNull();

            assertThat(format.nextRecord(u)).isNull();
            assertThat(format.reachedEnd()).isTrue();
        } finally {
            format.close();
        }
    }

    /**
     * Test if the AvroInputFormat is able to properly read data from an avro file as a
     * GenericRecord.
     *
     * @throws IOException if there is an error
     */
    @Test
    void testDeserializationGenericRecordReuseAvroValueFalse() throws IOException {
        Configuration parameters = new Configuration();

        AvroInputFormat<GenericRecord> format =
                new AvroInputFormat<>(new Path(testFile.getAbsolutePath()), GenericRecord.class);
        format.configure(parameters);
        format.setReuseAvroValue(false);

        doTestDeserializationGenericRecord(format, parameters);
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @AfterEach
    public void deleteFiles() {
        testFile.delete();
    }
}
