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

package org.apache.flink.api.common.typeutils;

import org.apache.flink.FlinkVersion;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.test.util.MigrationTest;

import org.assertj.core.api.Condition;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/** A test base for testing {@link TypeSerializer} upgrades. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class TypeSerializerUpgradeTestBase<PreviousElementT, UpgradedElementT>
        implements MigrationTest {

    // ------------------------------------------------------------------------------
    //  APIs
    // ------------------------------------------------------------------------------

    /**
     * Creates a collection of {@link TestSpecification} which will be used as input for
     * parametrized tests.
     */
    public abstract Collection<TestSpecification<?, ?>> createTestSpecifications(
            FlinkVersion currentVersion) throws Exception;

    public Collection<FlinkVersion> getMigrationVersions() {
        return FlinkVersion.rangeOf(
                FlinkVersion.v1_11, MigrationTest.getMostRecentlyPublishedVersion());
    }

    public final Collection<TestSpecification<?, ?>> createTestSpecificationsForAllVersions()
            throws Exception {
        List<TestSpecification<?, ?>> specificationList = new ArrayList<>();
        for (FlinkVersion version : getMigrationVersions()) {
            specificationList.addAll(createTestSpecifications(version));
        }
        return specificationList;
    }

    /**
     * Setup code for a {@link TestSpecification}. This creates the serializer before upgrade and
     * test data, that will be written by the created pre-upgrade {@link TypeSerializer}.
     */
    public interface PreUpgradeSetup<PreviousElementT> {

        /** Creates a pre-upgrade {@link TypeSerializer}. */
        TypeSerializer<PreviousElementT> createPriorSerializer();

        /** Creates test data that will be written using the pre-upgrade {@link TypeSerializer}. */
        PreviousElementT createTestData();
    }

    /**
     * Verification code for a {@link TestSpecification}. This creates the "upgraded" {@link
     * TypeSerializer} and provides matchers for comparing the deserialized test data and for the
     * {@link TypeSerializerSchemaCompatibility}.
     */
    public interface UpgradeVerifier<UpgradedElementT> {

        /** Creates a post-upgrade {@link TypeSerializer}. */
        TypeSerializer<UpgradedElementT> createUpgradedSerializer();

        /** Returns a {@link Condition} for asserting the deserialized test data. */
        Condition<UpgradedElementT> testDataCondition();

        /**
         * Returns a {@link Condition} for comparing the {@link TypeSerializerSchemaCompatibility}
         * that the serializer upgrade produced with an expected {@link
         * TypeSerializerSchemaCompatibility}.
         */
        Condition<TypeSerializerSchemaCompatibility<UpgradedElementT>> schemaCompatibilityCondition(
                FlinkVersion version);
    }

    private static class ClassLoaderSafePreUpgradeSetup<PreviousElementT>
            implements PreUpgradeSetup<PreviousElementT> {

        private final PreUpgradeSetup<PreviousElementT> delegateSetup;
        private final ClassLoader setupClassloader;

        ClassLoaderSafePreUpgradeSetup(
                Class<? extends PreUpgradeSetup<PreviousElementT>> delegateSetupClass)
                throws Exception {
            checkNotNull(delegateSetupClass);
            Class<? extends PreUpgradeSetup<PreviousElementT>> relocatedDelegateSetupClass =
                    ClassRelocator.relocate(delegateSetupClass);

            this.setupClassloader = relocatedDelegateSetupClass.getClassLoader();
            try (ThreadContextClassLoader ignored =
                    new ThreadContextClassLoader(setupClassloader)) {
                this.delegateSetup = relocatedDelegateSetupClass.newInstance();
            }
        }

        @Override
        public TypeSerializer<PreviousElementT> createPriorSerializer() {
            try (ThreadContextClassLoader ignored =
                    new ThreadContextClassLoader(setupClassloader)) {
                return delegateSetup.createPriorSerializer();
            }
        }

        @Override
        public PreviousElementT createTestData() {
            try (ThreadContextClassLoader ignored =
                    new ThreadContextClassLoader(setupClassloader)) {
                return delegateSetup.createTestData();
            }
        }
    }

    private static class ClassLoaderSafeUpgradeVerifier<UpgradedElementT>
            implements UpgradeVerifier<UpgradedElementT> {

        private final UpgradeVerifier<UpgradedElementT> delegateVerifier;
        private final ClassLoader verifierClassloader;

        ClassLoaderSafeUpgradeVerifier(
                Class<? extends UpgradeVerifier<UpgradedElementT>> delegateVerifierClass)
                throws Exception {
            checkNotNull(delegateVerifierClass);
            Class<? extends UpgradeVerifier<UpgradedElementT>> relocatedDelegateVerifierClass =
                    ClassRelocator.relocate(delegateVerifierClass);

            this.verifierClassloader = relocatedDelegateVerifierClass.getClassLoader();
            try (ThreadContextClassLoader ignored =
                    new ThreadContextClassLoader(verifierClassloader)) {
                this.delegateVerifier = relocatedDelegateVerifierClass.newInstance();
            }
        }

        @Override
        public TypeSerializer<UpgradedElementT> createUpgradedSerializer() {
            try (ThreadContextClassLoader ignored =
                    new ThreadContextClassLoader(verifierClassloader)) {
                return delegateVerifier.createUpgradedSerializer();
            }
        }

        @Override
        public Condition<UpgradedElementT> testDataCondition() {
            try (ThreadContextClassLoader ignored =
                    new ThreadContextClassLoader(verifierClassloader)) {
                return delegateVerifier.testDataCondition();
            }
        }

        @Override
        public Condition<TypeSerializerSchemaCompatibility<UpgradedElementT>>
                schemaCompatibilityCondition(FlinkVersion version) {
            try (ThreadContextClassLoader ignored =
                    new ThreadContextClassLoader(verifierClassloader)) {
                return delegateVerifier.schemaCompatibilityCondition(version);
            }
        }
    }

    /**
     * Specification of one test scenario. This mainly needs a {@link PreUpgradeSetup} and {@link
     * UpgradeVerifier}.
     */
    public static class TestSpecification<PreviousElementT, UpgradedElementT> {
        private final String name;
        private final FlinkVersion flinkVersion;
        private final ClassLoaderSafePreUpgradeSetup<PreviousElementT> setup;
        private final ClassLoaderSafeUpgradeVerifier<UpgradedElementT> verifier;

        public TestSpecification(
                String name,
                FlinkVersion flinkVersion,
                Class<? extends PreUpgradeSetup<PreviousElementT>> setupClass,
                Class<? extends UpgradeVerifier<UpgradedElementT>> verifierClass)
                throws Exception {
            this.name = checkNotNull(name);
            this.flinkVersion = checkNotNull(flinkVersion);
            this.setup = new ClassLoaderSafePreUpgradeSetup<>(setupClass);
            this.verifier = new ClassLoaderSafeUpgradeVerifier<>(verifierClass);
        }

        @Override
        public String toString() {
            return name + " / " + flinkVersion;
        }
    }

    // ------------------------------------------------------------------------------
    //  Test file generation
    // ------------------------------------------------------------------------------

    public static final int INITIAL_OUTPUT_BUFFER_SIZE = 64;

    /**
     * Execute this test to generate test files. Remember to be using the correct branch when
     * generating the test files, e.g. to generate test files for {@link FlinkVersion#v1_8}, you
     * should be under the release-1.8 branch.
     */
    @ParameterizedSnapshotsGenerator("createTestSpecifications")
    public void generateTestSetupFiles(
            TestSpecification<PreviousElementT, UpgradedElementT> testSpecification)
            throws Exception {
        Files.createDirectories(getSerializerSnapshotFilePath(testSpecification).getParent());

        try (ThreadContextClassLoader ignored =
                new ThreadContextClassLoader(testSpecification.setup.setupClassloader)) {
            TypeSerializer<PreviousElementT> priorSerializer =
                    testSpecification.setup.createPriorSerializer();

            // first, use the serializer to write test data
            // NOTE: it is important that we write test data first, because some serializers'
            // configuration
            //       mutates only after being used for serialization (e.g. dynamic type
            // registrations for Pojo / Kryo)
            DataOutputSerializer testDataOut = new DataOutputSerializer(INITIAL_OUTPUT_BUFFER_SIZE);
            priorSerializer.serialize(testSpecification.setup.createTestData(), testDataOut);
            writeContentsTo(
                    getGenerateDataFilePath(testSpecification), testDataOut.getCopyOfBuffer());

            // ... then write the serializer snapshot
            DataOutputSerializer serializerSnapshotOut =
                    new DataOutputSerializer(INITIAL_OUTPUT_BUFFER_SIZE);
            writeSerializerSnapshot(
                    serializerSnapshotOut, priorSerializer, testSpecification.flinkVersion);
            writeContentsTo(
                    getGenerateSerializerSnapshotFilePath(testSpecification),
                    serializerSnapshotOut.getCopyOfBuffer());
        }
    }

    // ------------------------------------------------------------------------------
    //  Tests
    // ------------------------------------------------------------------------------

    @ParameterizedTest(name = "Test Specification = {0}")
    @MethodSource("createTestSpecificationsForAllVersions")
    void restoreSerializerIsValid(
            TestSpecification<PreviousElementT, UpgradedElementT> testSpecification)
            throws Exception {
        try (ThreadContextClassLoader ignored =
                new ThreadContextClassLoader(testSpecification.verifier.verifierClassloader)) {
            assumeThat(
                            testSpecification
                                    .verifier
                                    .schemaCompatibilityCondition(testSpecification.flinkVersion)
                                    .matches(TypeSerializerSchemaCompatibility.incompatible()))
                    .as(
                            "This test only applies for test specifications that verify an upgraded serializer that is not incompatible.")
                    .isFalse();

            TypeSerializerSnapshot<UpgradedElementT> restoredSerializerSnapshot =
                    snapshotUnderTest(testSpecification);

            TypeSerializer<UpgradedElementT> restoredSerializer =
                    restoredSerializerSnapshot.restoreSerializer();
            assertSerializerIsValid(
                    restoredSerializer,
                    dataUnderTest(testSpecification),
                    testSpecification.verifier.testDataCondition());
        }
    }

    @ParameterizedTest(name = "Test Specification = {0}")
    @MethodSource("createTestSpecificationsForAllVersions")
    void upgradedSerializerHasExpectedSchemaCompatibility(
            TestSpecification<PreviousElementT, UpgradedElementT> testSpecification)
            throws Exception {
        try (ThreadContextClassLoader ignored =
                new ThreadContextClassLoader(testSpecification.verifier.verifierClassloader)) {
            TypeSerializerSnapshot<UpgradedElementT> restoredSerializerSnapshot =
                    snapshotUnderTest(testSpecification);
            TypeSerializer<UpgradedElementT> upgradedSerializer =
                    testSpecification.verifier.createUpgradedSerializer();

            TypeSerializerSchemaCompatibility<UpgradedElementT> upgradeCompatibility =
                    upgradedSerializer
                            .snapshotConfiguration()
                            .resolveSchemaCompatibility(restoredSerializerSnapshot);

            assertThat(upgradeCompatibility)
                    .is(
                            (testSpecification.verifier.schemaCompatibilityCondition(
                                    testSpecification.flinkVersion)));
        }
    }

    @ParameterizedTest(name = "Test Specification = {0}")
    @MethodSource("createTestSpecificationsForAllVersions")
    void upgradedSerializerIsValidAfterMigration(
            TestSpecification<PreviousElementT, UpgradedElementT> testSpecification)
            throws Exception {
        try (ThreadContextClassLoader ignored =
                new ThreadContextClassLoader(testSpecification.verifier.verifierClassloader)) {
            TypeSerializerSnapshot<UpgradedElementT> restoredSerializerSnapshot =
                    snapshotUnderTest(testSpecification);

            TypeSerializer<UpgradedElementT> upgradedSerializer =
                    testSpecification.verifier.createUpgradedSerializer();

            TypeSerializerSchemaCompatibility<UpgradedElementT> upgradeCompatibility =
                    upgradedSerializer
                            .snapshotConfiguration()
                            .resolveSchemaCompatibility(restoredSerializerSnapshot);
            assumeThat(upgradeCompatibility)
                    .as(
                            "This test only applies for test specifications that verify an upgraded serializer that requires migration to be compatible.")
                    .is(TypeSerializerConditions.isCompatibleAfterMigration());

            // migrate the previous data schema,
            TypeSerializer<UpgradedElementT> restoreSerializer =
                    restoredSerializerSnapshot.restoreSerializer();
            DataInputView migratedData =
                    readAndThenWriteData(
                            dataUnderTest(testSpecification),
                            restoreSerializer,
                            upgradedSerializer,
                            testSpecification.verifier.testDataCondition());

            // .. and then assert that the upgraded serializer is valid with the migrated data
            assertSerializerIsValid(
                    upgradedSerializer,
                    migratedData,
                    testSpecification.verifier.testDataCondition());
        }
    }

    @ParameterizedTest(name = "Test Specification = {0}")
    @MethodSource("createTestSpecificationsForAllVersions")
    void upgradedSerializerIsValidAfterReconfiguration(
            TestSpecification<PreviousElementT, UpgradedElementT> testSpecification)
            throws Exception {
        try (ThreadContextClassLoader ignored =
                new ThreadContextClassLoader(testSpecification.verifier.verifierClassloader)) {
            TypeSerializerSnapshot<UpgradedElementT> restoredSerializerSnapshot =
                    snapshotUnderTest(testSpecification);
            TypeSerializer<UpgradedElementT> upgradedSerializer =
                    testSpecification.verifier.createUpgradedSerializer();

            TypeSerializerSchemaCompatibility<UpgradedElementT> upgradeCompatibility =
                    upgradedSerializer
                            .snapshotConfiguration()
                            .resolveSchemaCompatibility(restoredSerializerSnapshot);
            assumeThat(upgradeCompatibility)
                    .as(
                            "This test only applies for test specifications that verify an upgraded serializer that requires reconfiguration to be compatible.")
                    .is(TypeSerializerConditions.isCompatibleWithReconfiguredSerializer());

            TypeSerializer<UpgradedElementT> reconfiguredUpgradedSerializer =
                    upgradeCompatibility.getReconfiguredSerializer();
            assertSerializerIsValid(
                    reconfiguredUpgradedSerializer,
                    dataUnderTest(testSpecification),
                    testSpecification.verifier.testDataCondition());
        }
    }

    @ParameterizedTest(name = "Test Specification = {0}")
    @MethodSource("createTestSpecificationsForAllVersions")
    void upgradedSerializerIsValidWhenCompatibleAsIs(
            TestSpecification<PreviousElementT, UpgradedElementT> testSpecification)
            throws Exception {
        try (ThreadContextClassLoader ignored =
                new ThreadContextClassLoader(testSpecification.verifier.verifierClassloader)) {
            TypeSerializerSnapshot<UpgradedElementT> restoredSerializerSnapshot =
                    snapshotUnderTest(testSpecification);
            TypeSerializer<UpgradedElementT> upgradedSerializer =
                    testSpecification.verifier.createUpgradedSerializer();

            TypeSerializerSchemaCompatibility<UpgradedElementT> upgradeCompatibility =
                    upgradedSerializer
                            .snapshotConfiguration()
                            .resolveSchemaCompatibility(restoredSerializerSnapshot);
            assumeThat(upgradeCompatibility)
                    .as(
                            "This test only applies for test specifications that verify an upgraded serializer that is compatible as is.")
                    .is((TypeSerializerConditions.isCompatibleAsIs()));

            assertSerializerIsValid(
                    upgradedSerializer,
                    dataUnderTest(testSpecification),
                    testSpecification.verifier.testDataCondition());
        }
    }

    /**
     * Asserts that a given {@link TypeSerializer} is valid, given a {@link DataInputView} of
     * serialized data.
     *
     * <p>A serializer is valid, iff:
     *
     * <ul>
     *   <li>1. The serializer can read and then write again the given serialized data.
     *   <li>2. The serializer can produce a serializer snapshot which can be written and then read
     *       back again.
     *   <li>3. The serializer's produced snapshot is capable of creating a restore serializer.
     *   <li>4. The restore serializer created from the serializer snapshot can read and then write
     *       again data written by step 1. Given that the serializer is not a restore serializer
     *       already.
     * </ul>
     */
    private <T> void assertSerializerIsValid(
            TypeSerializer<T> serializer, DataInputView dataInput, Condition<T> testDataMatcher)
            throws Exception {

        DataInputView serializedData =
                readAndThenWriteData(dataInput, serializer, serializer, testDataMatcher);
        TypeSerializerSnapshot<T> snapshot = writeAndThenReadSerializerSnapshot(serializer);
        TypeSerializer<T> restoreSerializer = snapshot.restoreSerializer();
        serializedData =
                readAndThenWriteData(
                        serializedData, restoreSerializer, restoreSerializer, testDataMatcher);

        TypeSerializer<T> duplicateSerializer = snapshot.restoreSerializer().duplicate();
        readAndThenWriteData(
                serializedData, duplicateSerializer, duplicateSerializer, testDataMatcher);
    }

    // ------------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------------

    /** Paths to use during snapshot generation, which should only use the CURRENT_VERSION. */
    private Path getGenerateSerializerSnapshotFilePath(
            TestSpecification<PreviousElementT, UpgradedElementT> testSpecification) {
        return Paths.get(getGenerateResourceDirectory(testSpecification) + "/serializer-snapshot");
    }

    /** Paths to use during snapshot generation, which should only use the CURRENT_VERSION. */
    private Path getGenerateDataFilePath(
            TestSpecification<PreviousElementT, UpgradedElementT> testSpecification) {
        return Paths.get(getGenerateResourceDirectory(testSpecification) + "/test-data");
    }

    /** Paths to use during snapshot generation, which should only use the CURRENT_VERSION. */
    private String getGenerateResourceDirectory(
            TestSpecification<PreviousElementT, UpgradedElementT> testSpecification) {
        return System.getProperty("user.dir")
                + "/src/test/resources/"
                + testSpecification.name
                + "-"
                + testSpecification.flinkVersion;
    }

    private Path getSerializerSnapshotFilePath(
            TestSpecification<PreviousElementT, UpgradedElementT> testSpecification) {
        return Paths.get(getTestResourceDirectory(testSpecification) + "/serializer-snapshot");
    }

    private Path getTestDataFilePath(
            TestSpecification<PreviousElementT, UpgradedElementT> testSpecification) {
        return Paths.get(getTestResourceDirectory(testSpecification) + "/test-data");
    }

    private String getTestResourceDirectory(
            TestSpecification<PreviousElementT, UpgradedElementT> testSpecification) {
        return System.getProperty("user.dir")
                + "/src/test/resources/"
                + testSpecification.name
                + "-"
                + testSpecification.flinkVersion;
    }

    private TypeSerializerSnapshot<UpgradedElementT> snapshotUnderTest(
            TestSpecification<PreviousElementT, UpgradedElementT> testSpecification)
            throws Exception {
        return readSerializerSnapshot(
                contentsOf(getSerializerSnapshotFilePath(testSpecification)),
                testSpecification.flinkVersion);
    }

    private DataInputView dataUnderTest(
            TestSpecification<PreviousElementT, UpgradedElementT> testSpecification) {
        return contentsOf(getTestDataFilePath(testSpecification));
    }

    private static void writeContentsTo(Path path, byte[] bytes) {
        try {
            Files.write(path, bytes);
        } catch (IOException e) {
            throw new RuntimeException("Failed to write to " + path, e);
        }
    }

    private static DataInputView contentsOf(Path path) {
        try {
            byte[] bytes = Files.readAllBytes(path);
            return new DataInputDeserializer(bytes);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read contents of " + path, e);
        }
    }

    private static <T> void writeSerializerSnapshot(
            DataOutputView out, TypeSerializer<T> serializer, FlinkVersion flinkVersion)
            throws IOException {

        if (flinkVersion.isNewerVersionThan(FlinkVersion.v1_6)) {
            writeSerializerSnapshotCurrentFormat(out, serializer);
        } else {
            throw new UnsupportedOperationException(
                    "There should be no longer a need to support/use this path since Flink 1.17");
        }
    }

    private static <T> void writeSerializerSnapshotCurrentFormat(
            DataOutputView out, TypeSerializer<T> serializer) throws IOException {

        TypeSerializerSnapshotSerializationUtil.writeSerializerSnapshot(
                out, serializer.snapshotConfiguration());
    }

    private static <T> TypeSerializerSnapshot<T> readSerializerSnapshot(
            DataInputView in, FlinkVersion flinkVersion) throws IOException {

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        checkState(flinkVersion.isNewerVersionThan(FlinkVersion.v1_6));
        return readSerializerSnapshotCurrentFormat(in, classLoader);
    }

    private static <T> TypeSerializerSnapshot<T> readSerializerSnapshotCurrentFormat(
            DataInputView in, ClassLoader userCodeClassLoader) throws IOException {

        return TypeSerializerSnapshotSerializationUtil.readSerializerSnapshot(
                in, userCodeClassLoader);
    }

    public <T> DataInputView readAndThenWriteData(
            DataInputView originalDataInput,
            TypeSerializer<T> readSerializer,
            TypeSerializer<T> writeSerializer,
            Condition<T> testDataCondition)
            throws IOException {

        T data = readSerializer.deserialize(originalDataInput);
        assertThat(data).is(testDataCondition);

        DataOutputSerializer out = new DataOutputSerializer(INITIAL_OUTPUT_BUFFER_SIZE);
        writeSerializer.serialize(data, out);
        return new DataInputDeserializer(out.wrapAsByteBuffer());
    }

    private static <T> TypeSerializerSnapshot<T> writeAndThenReadSerializerSnapshot(
            TypeSerializer<T> serializer) throws IOException {

        DataOutputSerializer out = new DataOutputSerializer(INITIAL_OUTPUT_BUFFER_SIZE);
        writeSerializerSnapshotCurrentFormat(out, serializer);

        DataInputDeserializer in = new DataInputDeserializer(out.wrapAsByteBuffer());
        return readSerializerSnapshotCurrentFormat(
                in, Thread.currentThread().getContextClassLoader());
    }
}
