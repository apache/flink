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

package org.apache.flink.formats.avro.typeutils;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshotSerializationUtil;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.formats.avro.generated.Address;
import org.apache.flink.formats.avro.generated.User;
import org.apache.flink.formats.avro.utils.TestDataGenerator;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;

import static org.apache.flink.api.common.typeutils.TypeSerializerMatchers.isCompatibleAfterMigration;
import static org.apache.flink.api.common.typeutils.TypeSerializerMatchers.isCompatibleAsIs;
import static org.apache.flink.api.common.typeutils.TypeSerializerMatchers.isIncompatible;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.HamcrestCondition.matching;

/** Test {@link AvroSerializerSnapshot}. */
class AvroSerializerSnapshotTest {

    private static final int[] PAST_VERSIONS = new int[] {2};

    private static final Schema FIRST_NAME =
            SchemaBuilder.record("name")
                    .namespace("org.apache.flink")
                    .fields()
                    .requiredString("first")
                    .endRecord();

    private static final Schema FIRST_REQUIRED_LAST_OPTIONAL =
            SchemaBuilder.record("name")
                    .namespace("org.apache.flink")
                    .fields()
                    .requiredString("first")
                    .optionalString("last")
                    .endRecord();

    private static final Schema BOTH_REQUIRED =
            SchemaBuilder.record("name")
                    .namespace("org.apache.flink")
                    .fields()
                    .requiredString("first")
                    .requiredString("last")
                    .endRecord();

    @Test
    void sameSchemaShouldBeCompatibleAsIs() {
        assertThat(AvroSerializerSnapshot.resolveSchemaCompatibility(FIRST_NAME, FIRST_NAME))
                .is(matching(isCompatibleAsIs()));
    }

    @Test
    void removingAnOptionalFieldsIsCompatibleAsIs() {
        assertThat(
                        AvroSerializerSnapshot.resolveSchemaCompatibility(
                                FIRST_REQUIRED_LAST_OPTIONAL, FIRST_NAME))
                .is(matching(isCompatibleAfterMigration()));
    }

    @Test
    void addingAnOptionalFieldsIsCompatibleAsIs() {
        assertThat(
                        AvroSerializerSnapshot.resolveSchemaCompatibility(
                                FIRST_NAME, FIRST_REQUIRED_LAST_OPTIONAL))
                .is(matching(isCompatibleAfterMigration()));
    }

    @Test
    void addingARequiredMakesSerializersIncompatible() {
        assertThat(
                        AvroSerializerSnapshot.resolveSchemaCompatibility(
                                FIRST_REQUIRED_LAST_OPTIONAL, BOTH_REQUIRED))
                .is(matching(isIncompatible()));
    }

    @Test
    void anAvroSnapshotIsCompatibleWithItsOriginatingSerializer() {
        AvroSerializer<GenericRecord> serializer =
                new AvroSerializer<>(GenericRecord.class, FIRST_REQUIRED_LAST_OPTIONAL);

        TypeSerializerSnapshot<GenericRecord> snapshot = serializer.snapshotConfiguration();

        assertThat(snapshot.resolveSchemaCompatibility(serializer))
                .is(matching(isCompatibleAsIs()));
    }

    @Test
    void anAvroSnapshotIsCompatibleAfterARoundTrip() throws IOException {
        AvroSerializer<GenericRecord> serializer =
                new AvroSerializer<>(GenericRecord.class, FIRST_REQUIRED_LAST_OPTIONAL);

        AvroSerializerSnapshot<GenericRecord> restored =
                roundTrip(serializer.snapshotConfiguration());

        assertThat(restored.resolveSchemaCompatibility(serializer))
                .is(matching(isCompatibleAsIs()));
    }

    @Test
    void anAvroSpecificRecordIsCompatibleAfterARoundTrip() throws IOException {
        // user is an avro generated test object.
        AvroSerializer<User> serializer = new AvroSerializer<>(User.class);

        AvroSerializerSnapshot<User> restored = roundTrip(serializer.snapshotConfiguration());

        assertThat(restored.resolveSchemaCompatibility(serializer))
                .is(matching(isCompatibleAsIs()));
    }

    @Test
    void aPojoIsCompatibleAfterARoundTrip() throws IOException {
        AvroSerializer<Pojo> serializer = new AvroSerializer<>(Pojo.class);

        AvroSerializerSnapshot<Pojo> restored = roundTrip(serializer.snapshotConfiguration());

        assertThat(restored.resolveSchemaCompatibility(serializer))
                .is(matching(isCompatibleAsIs()));
    }

    @Test
    void aLargeSchemaAvroSnapshotIsCompatibleAfterARoundTrip() throws IOException {
        // construct the large schema up to a size of 65535 bytes.
        int thresholdSize = 65535;
        StringBuilder schemaField = new StringBuilder(thresholdSize);
        for (int i = 0; i <= thresholdSize; i++) {
            schemaField.append('a');
        }
        Schema largeSchema =
                SchemaBuilder.record("name")
                        .namespace("org.apache.flink")
                        .fields()
                        .requiredString(schemaField.toString())
                        .endRecord();

        AvroSerializer<GenericRecord> serializer =
                new AvroSerializer<>(GenericRecord.class, largeSchema);
        AvroSerializerSnapshot<GenericRecord> restored =
                roundTrip(serializer.snapshotConfiguration());

        assertThat(restored.resolveSchemaCompatibility(serializer))
                .is(matching(isCompatibleAsIs()));
    }

    @Test
    void recordSerializedShouldBeDeserializeWithTheResortedSerializer() throws IOException {
        // user is an avro generated test object.
        final User user = TestDataGenerator.generateRandomUser(new Random());
        final AvroSerializer<User> originalSerializer = new AvroSerializer<>(User.class);
        //
        // first serialize the record
        //
        ByteBuffer serializedUser = serialize(originalSerializer, user);
        //
        // then restore a serializer from the snapshot
        //
        TypeSerializer<User> restoredSerializer =
                originalSerializer.snapshotConfiguration().restoreSerializer();
        //
        // now deserialize the user with the resorted serializer.
        //
        User restoredUser = deserialize(restoredSerializer, serializedUser);

        assertThat(restoredUser).isEqualTo(user);
    }

    @Test
    void validSchemaEvaluationShouldResultInCRequiresMigration() {
        final AvroSerializer<GenericRecord> originalSerializer =
                new AvroSerializer<>(GenericRecord.class, FIRST_NAME);
        final AvroSerializer<GenericRecord> newSerializer =
                new AvroSerializer<>(GenericRecord.class, FIRST_REQUIRED_LAST_OPTIONAL);

        TypeSerializerSnapshot<GenericRecord> originalSnapshot =
                originalSerializer.snapshotConfiguration();

        assertThat(originalSnapshot.resolveSchemaCompatibility(newSerializer))
                .is(matching(isCompatibleAfterMigration()));
    }

    @Test
    void nonValidSchemaEvaluationShouldResultInCompatibleSerializers() {
        final AvroSerializer<GenericRecord> originalSerializer =
                new AvroSerializer<>(GenericRecord.class, FIRST_REQUIRED_LAST_OPTIONAL);
        final AvroSerializer<GenericRecord> newSerializer =
                new AvroSerializer<>(GenericRecord.class, BOTH_REQUIRED);

        TypeSerializerSnapshot<GenericRecord> originalSnapshot =
                originalSerializer.snapshotConfiguration();

        assertThat(originalSnapshot.resolveSchemaCompatibility(newSerializer))
                .is(matching(isIncompatible()));
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void
            changingFromGenericToSpecificWithCompatibleSchemaShouldResultInCompatibleSerializers() {
        // starting with a generic serializer
        AvroSerializer<Object> generic = new AvroSerializer(GenericRecord.class, User.SCHEMA$);
        TypeSerializerSnapshot<Object> genericSnapshot = generic.snapshotConfiguration();

        // then upgrading to a specific serializer
        AvroSerializer<Object> specificSerializer = new AvroSerializer(User.class);
        specificSerializer.snapshotConfiguration();

        assertThat(genericSnapshot.resolveSchemaCompatibility(specificSerializer))
                .is(matching(isCompatibleAsIs()));
    }

    @Test
    void restorePastSnapshots() throws IOException {
        for (int pastVersion : PAST_VERSIONS) {
            AvroSerializer<GenericRecord> currentSerializer =
                    new AvroSerializer<>(GenericRecord.class, Address.getClassSchema());

            DataInputView in =
                    new DataInputDeserializer(
                            Files.readAllBytes(getSerializerSnapshotFilePath(pastVersion)));

            TypeSerializerSnapshot<GenericRecord> restored =
                    TypeSerializerSnapshotSerializationUtil.readSerializerSnapshot(
                            in, AvroSerializer.class.getClassLoader(), null);

            assertThat(restored.resolveSchemaCompatibility(currentSerializer))
                    .is(matching(isCompatibleAsIs()));
        }
    }

    /**
     * Creates a new serializer snapshot for the current version. Use this before bumping the
     * snapshot version and also add the version (before bumping) to {@link #PAST_VERSIONS}.
     */
    @Disabled
    @Test
    void writeCurrentVersionSnapshot() throws IOException {
        AvroSerializer<GenericRecord> serializer =
                new AvroSerializer<>(GenericRecord.class, Address.getClassSchema());

        DataOutputSerializer out = new DataOutputSerializer(1024);

        TypeSerializerSnapshotSerializationUtil.writeSerializerSnapshot(
                out, serializer.snapshotConfiguration(), serializer);

        Path snapshotPath =
                getSerializerSnapshotFilePath(new AvroSerializerSnapshot<>().getCurrentVersion());

        Files.write(snapshotPath, out.getCopyOfBuffer());
    }

    private Path getSerializerSnapshotFilePath(int version) {
        return Paths.get(
                System.getProperty("user.dir")
                        + "/src/test/resources/serializer-snapshot-v"
                        + version);
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Utils
    // ---------------------------------------------------------------------------------------------------------------

    /** Serialize an (avro)TypeSerializerSnapshot and deserialize it. */
    private static <T> AvroSerializerSnapshot<T> roundTrip(TypeSerializerSnapshot<T> original)
            throws IOException {
        // writeSnapshot();
        DataOutputSerializer out = new DataOutputSerializer(1024);
        original.writeSnapshot(out);

        // init
        AvroSerializerSnapshot<T> restored = new AvroSerializerSnapshot<>();

        // readSnapshot();
        DataInputView in = new DataInputDeserializer(out.wrapAsByteBuffer());
        restored.readSnapshot(
                restored.getCurrentVersion(), in, original.getClass().getClassLoader());

        return restored;
    }

    private static <T> ByteBuffer serialize(TypeSerializer<T> serializer, T record)
            throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(1024);
        serializer.serialize(record, out);
        return out.wrapAsByteBuffer();
    }

    private static <T> T deserialize(TypeSerializer<T> serializer, ByteBuffer serializedRecord)
            throws IOException {
        DataInputView in = new DataInputDeserializer(serializedRecord);
        return serializer.deserialize(in);
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Test classes
    // ---------------------------------------------------------------------------------------------------------------

    private static class Pojo {
        private String foo;

        public String getFoo() {
            return foo;
        }

        public void setFoo(String foo) {
            this.foo = foo;
        }
    }
}
