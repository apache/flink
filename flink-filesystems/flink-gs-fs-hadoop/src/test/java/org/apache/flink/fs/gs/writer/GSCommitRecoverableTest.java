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

package org.apache.flink.fs.gs.writer;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.fs.gs.GSFileSystemOptions;
import org.apache.flink.fs.gs.storage.GSBlobIdentifier;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test {@link GSResumeRecoverable}. */
@ExtendWith(ParameterizedTestExtension.class)
class GSCommitRecoverableTest {

    @Parameter private List<UUID> componentObjectIds;

    @Parameter(value = 1)
    private String temporaryBucketName;

    @Parameters(name = "componentObjectIds={0}, temporaryBucketName={1}")
    private static Collection<Object[]> data() {

        ArrayList<UUID> emptyComponentObjectIds = new ArrayList<>();
        ArrayList<UUID> populatedComponentObjectIds = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            populatedComponentObjectIds.add(UUID.randomUUID());
        }
        GSBlobIdentifier blobIdentifier = new GSBlobIdentifier("foo", "bar");

        return Arrays.asList(
                new Object[][] {
                    // no component ids with no temporary bucket specified
                    {emptyComponentObjectIds, null},
                    // no component ids with a temporary bucket specified
                    {emptyComponentObjectIds, "temporary-bucket"},
                    // populated component ids with no temporary bucket specified
                    {populatedComponentObjectIds, null},
                    //  populated component ids with temporary bucket specified
                    {populatedComponentObjectIds, "temporary-bucket"},
                });
    }

    private GSBlobIdentifier blobIdentifier;

    @BeforeEach
    void before() {
        blobIdentifier = new GSBlobIdentifier("foo", "bar");
    }

    @TestTemplate
    void shouldConstructProperly() {
        GSCommitRecoverable commitRecoverable =
                new GSCommitRecoverable(blobIdentifier, componentObjectIds);
        assertThat(commitRecoverable.finalBlobIdentifier).isEqualTo(blobIdentifier);
        assertThat(commitRecoverable.componentObjectIds).isEqualTo(componentObjectIds);
    }

    /** Ensure that the list of component object ids cannot be added to. */
    @TestTemplate
    void shouldNotAddComponentId() {
        GSCommitRecoverable commitRecoverable =
                new GSCommitRecoverable(blobIdentifier, componentObjectIds);
        assertThatThrownBy(() -> commitRecoverable.componentObjectIds.add(UUID.randomUUID()))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    /** Ensure that component object ids can't be updated. */
    @TestTemplate
    void shouldNotModifyComponentId() {
        GSCommitRecoverable commitRecoverable =
                new GSCommitRecoverable(blobIdentifier, componentObjectIds);

        assertThatThrownBy(() -> commitRecoverable.componentObjectIds.set(0, UUID.randomUUID()))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @TestTemplate
    void shouldGetComponentBlobIds() {

        // configure options, if this test configuration has a temporary bucket name, set it
        Configuration flinkConfig = new Configuration();
        if (temporaryBucketName != null) {
            flinkConfig.set(GSFileSystemOptions.WRITER_TEMPORARY_BUCKET_NAME, temporaryBucketName);
        }
        GSFileSystemOptions options = new GSFileSystemOptions(flinkConfig);
        GSCommitRecoverable commitRecoverable =
                new GSCommitRecoverable(blobIdentifier, componentObjectIds);
        List<GSBlobIdentifier> componentBlobIdentifiers =
                commitRecoverable.getComponentBlobIds(options);

        for (int i = 0; i < componentObjectIds.size(); i++) {
            UUID componentObjectId = componentObjectIds.get(i);
            GSBlobIdentifier componentBlobIdentifier = componentBlobIdentifiers.get(i);

            // if a temporary bucket is specified in options, the component blob identifier
            // should be in this bucket; otherwise, it should be in the bucket with the final blob
            assertThat(componentBlobIdentifier.bucketName)
                    .isEqualTo(
                            temporaryBucketName == null
                                    ? blobIdentifier.bucketName
                                    : temporaryBucketName);

            // make sure the name is what is expected
            String expectedObjectName =
                    String.format(
                            ".inprogress/%s/%s/%s",
                            blobIdentifier.bucketName,
                            blobIdentifier.objectName,
                            componentObjectId);
            assertThat(componentBlobIdentifier.objectName).isEqualTo(expectedObjectName);
        }
    }

    @TestTemplate
    void shouldGetComponentBlobIdsWithEntropy() {

        // configure options, if this test configuration has a temporary bucket name, set it
        Configuration flinkConfig = new Configuration();
        if (temporaryBucketName != null) {
            flinkConfig.set(GSFileSystemOptions.WRITER_TEMPORARY_BUCKET_NAME, temporaryBucketName);
        }
        // enable filesink entropy
        flinkConfig.set(GSFileSystemOptions.ENABLE_FILESINK_ENTROPY, Boolean.TRUE);
        GSFileSystemOptions options = new GSFileSystemOptions(flinkConfig);
        GSCommitRecoverable commitRecoverable =
                new GSCommitRecoverable(blobIdentifier, componentObjectIds);
        List<GSBlobIdentifier> componentBlobIdentifiers =
                commitRecoverable.getComponentBlobIds(options);

        for (int i = 0; i < componentObjectIds.size(); i++) {
            UUID componentObjectId = componentObjectIds.get(i);
            GSBlobIdentifier componentBlobIdentifier = componentBlobIdentifiers.get(i);

            // if a temporary bucket is specified in options, the component blob identifier
            // should be in this bucket; otherwise, it should be in the bucket with the final blob
            assertThat(componentBlobIdentifier.bucketName)
                    .isEqualTo(
                            temporaryBucketName == null
                                    ? blobIdentifier.bucketName
                                    : temporaryBucketName);

            // make sure the name is what is expected
            String expectedObjectName =
                    String.format(
                            "%s.inprogress/%s/%s/%s",
                            componentObjectId,
                            blobIdentifier.bucketName,
                            blobIdentifier.objectName,
                            componentObjectId);
            assertThat(componentBlobIdentifier.objectName).isEqualTo(expectedObjectName);
        }
    }
}
