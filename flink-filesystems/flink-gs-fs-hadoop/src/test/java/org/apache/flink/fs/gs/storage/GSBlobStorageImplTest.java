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

package org.apache.flink.fs.gs.storage;

import org.apache.flink.configuration.MemorySize;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Test {@link GSBlobStorageImpl}. */
class GSBlobStorageImplTest {

    private static final String TEST_BUCKET = "test-bucket";
    private GSBlobStorageImpl blobStorage;
    private Storage storage;

    @BeforeEach
    void setUp() {
        storage = LocalStorageHelper.getOptions().getService();
        blobStorage = new GSBlobStorageImpl(storage);
    }

    @ParameterizedTest(name = "{0} with null BlobInfo")
    @MethodSource("provideOptionTypes")
    void testGetBlobOptionWithNullBlobInfo(
            String optionType,
            Supplier<Object> doesNotExistSupplier,
            Function<Long, Object> generationMatchFunction,
            Object expectedDoesNotExist) {
        // When blob doesn't exist (null), should return doesNotExist option
        Object result =
                blobStorage.getBlobOption(null, doesNotExistSupplier, generationMatchFunction);

        assertThat(result).isNotNull();
        assertThat(result).isEqualTo(expectedDoesNotExist);
    }

    @ParameterizedTest(name = "{0} with existing BlobInfo")
    @MethodSource("provideOptionTypes")
    void testGetBlobOptionWithExistingBlobInfo(
            String optionType,
            Supplier<Object> doesNotExistSupplier,
            Function<Long, Object> generationMatchFunction,
            Object expectedDoesNotExist) {
        // Create a BlobInfo with a generation number
        Long generation = 12345L;
        BlobId blobId = BlobId.of("test-bucket", "test-object", generation);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();

        // When blob exists, should return generationMatch option
        Object result =
                blobStorage.getBlobOption(blobInfo, doesNotExistSupplier, generationMatchFunction);

        assertThat(result).isNotNull();
        assertThat(result.toString()).contains(generation.toString());
    }

    @ParameterizedTest(name = "{0} with zero generation")
    @MethodSource("provideOptionTypes")
    void testGetBlobOptionWithZeroGeneration(
            String optionType,
            Supplier<Object> doesNotExistSupplier,
            Function<Long, Object> generationMatchFunction,
            Object expectedDoesNotExist) {
        // Test edge case: blob with generation 0
        BlobId blobId = BlobId.of("test-bucket", "test-object", 0L);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();

        Object result =
                blobStorage.getBlobOption(blobInfo, doesNotExistSupplier, generationMatchFunction);

        assertThat(result).isNotNull();
        assertThat(result.toString()).contains("0");
    }

    @Test
    void testWriteBlob() throws IOException {
        // Note: LocalStorageHelper doesn't properly track blob generation numbers,
        // so we can only test writing to new blobs in this test
        GSBlobIdentifier blobIdentifier = new GSBlobIdentifier(TEST_BUCKET, "test-blob");

        // Write to the blob
        GSBlobStorage.WriteChannel writeChannel = blobStorage.writeBlob(blobIdentifier);

        assertThat(writeChannel).isNotNull();

        // Write some data
        byte[] data = "test data".getBytes();
        int written = writeChannel.write(data, 0, data.length);
        assertThat(written).isEqualTo(data.length);

        writeChannel.close();

        // Verify the blob was created
        Blob blob = storage.get(blobIdentifier.getBlobId());
        assertThat(blob).isNotNull();
        assertThat(blob.getContent()).isEqualTo(data);
    }

    @Test
    void testWriteBlobWithChunkSize() throws IOException {
        GSBlobIdentifier blobIdentifier = new GSBlobIdentifier(TEST_BUCKET, "chunked-blob");
        MemorySize chunkSize = MemorySize.parse("2b");

        GSBlobStorage.WriteChannel writeChannel = blobStorage.writeBlob(blobIdentifier, chunkSize);

        assertThat(writeChannel).isNotNull();

        byte[] data = "test data with chunks".getBytes();
        int written = writeChannel.write(data, 0, data.length);
        assertThat(written).isEqualTo(data.length);

        writeChannel.close();

        // Verify the blob was created
        Blob blob = storage.get(blobIdentifier.getBlobId());
        assertThat(blob).isNotNull();
        assertThat(blob.getContent()).isEqualTo(data);
    }

    @Test
    void testComposeMultipleBlobs() {
        // Note: LocalStorageHelper doesn't fully implement the compose operation,
        // so we can only verify that the method completes without error

        // Create source blobs with content
        GSBlobIdentifier source1 = new GSBlobIdentifier(TEST_BUCKET, "source1");
        GSBlobIdentifier source2 = new GSBlobIdentifier(TEST_BUCKET, "source2");
        GSBlobIdentifier source3 = new GSBlobIdentifier(TEST_BUCKET, "source3");

        byte[] data1 = "part1".getBytes();
        byte[] data2 = "part2".getBytes();
        byte[] data3 = "part3".getBytes();

        storage.create(BlobInfo.newBuilder(source1.getBlobId()).build(), data1);
        storage.create(BlobInfo.newBuilder(source2.getBlobId()).build(), data2);
        storage.create(BlobInfo.newBuilder(source3.getBlobId()).build(), data3);

        // Verify source blobs exist
        assertThat(storage.get(source1.getBlobId())).isNotNull();
        assertThat(storage.get(source2.getBlobId())).isNotNull();
        assertThat(storage.get(source3.getBlobId())).isNotNull();

        // Compose into target blob - this should complete without throwing an exception
        GSBlobIdentifier target = new GSBlobIdentifier(TEST_BUCKET, "composed-target");
        blobStorage.compose(java.util.Arrays.asList(source1, source2, source3), target);

        // Verify the method completed (no exception thrown)
        // The actual composition result cannot be verified with LocalStorageHelper
    }

    @Test
    void testComposeSingleBlob() {
        // Note: LocalStorageHelper doesn't fully implement the compose operation,
        // so we can only verify that the method completes without error

        // Create a single source blob
        GSBlobIdentifier source = new GSBlobIdentifier(TEST_BUCKET, "single-source");
        byte[] data = "single blob content".getBytes();
        storage.create(BlobInfo.newBuilder(source.getBlobId()).build(), data);

        // Verify source blob exists
        assertThat(storage.get(source.getBlobId())).isNotNull();

        // Compose into target (effectively a copy) - should complete without throwing
        GSBlobIdentifier target = new GSBlobIdentifier(TEST_BUCKET, "single-target");
        blobStorage.compose(java.util.Collections.singletonList(source), target);

        // Verify the method completed (no exception thrown)
        // The actual composition result cannot be verified with LocalStorageHelper
    }

    private static Stream<Arguments> provideOptionTypes() {
        return Stream.of(
                Arguments.of(
                        "BlobTargetOption",
                        (Supplier<Object>) Storage.BlobTargetOption::doesNotExist,
                        (Function<Long, Object>) Storage.BlobTargetOption::generationMatch,
                        Storage.BlobTargetOption.doesNotExist()),
                Arguments.of(
                        "BlobWriteOption",
                        (Supplier<Object>) Storage.BlobWriteOption::doesNotExist,
                        (Function<Long, Object>) Storage.BlobWriteOption::generationMatch,
                        Storage.BlobWriteOption.doesNotExist()));
    }
}
