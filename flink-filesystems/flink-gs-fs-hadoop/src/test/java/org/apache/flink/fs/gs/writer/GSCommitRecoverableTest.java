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

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

/** Test {@link GSResumeRecoverable}. */
@RunWith(Parameterized.class)
public class GSCommitRecoverableTest {

    @Parameterized.Parameter(value = 0)
    public List<UUID> componentObjectIds;

    @Parameterized.Parameter(value = 1)
    public String temporaryBucketName;

    @Parameterized.Parameters(name = "componentObjectIds={0}, temporaryBucketName={1}")
    public static Collection<Object[]> data() {

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

    @Before
    public void before() {
        blobIdentifier = new GSBlobIdentifier("foo", "bar");
    }

    @Test
    public void shouldConstructProperly() {
        GSCommitRecoverable commitRecoverable =
                new GSCommitRecoverable(blobIdentifier, componentObjectIds);
        assertEquals(blobIdentifier, commitRecoverable.finalBlobIdentifier);
        assertEquals(componentObjectIds, commitRecoverable.componentObjectIds);
    }

    /** Ensure that the list of component object ids cannot be added to. */
    @Test(expected = UnsupportedOperationException.class)
    public void shouldNotAddComponentId() {
        GSCommitRecoverable commitRecoverable =
                new GSCommitRecoverable(blobIdentifier, componentObjectIds);
        commitRecoverable.componentObjectIds.add(UUID.randomUUID());
    }

    /** Ensure that component object ids can't be updated. */
    @Test(expected = UnsupportedOperationException.class)
    public void shouldNotModifyComponentId() {
        GSCommitRecoverable commitRecoverable =
                new GSCommitRecoverable(blobIdentifier, componentObjectIds);
        commitRecoverable.componentObjectIds.set(0, UUID.randomUUID());
    }

    @Test
    public void shouldGetComponentBlobIds() {

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
            assertEquals(
                    temporaryBucketName == null ? blobIdentifier.bucketName : temporaryBucketName,
                    componentBlobIdentifier.bucketName);

            // make sure the name is what is expected
            String expectedObjectName =
                    String.format(
                            ".inprogress/%s/%s/%s",
                            blobIdentifier.bucketName,
                            blobIdentifier.objectName,
                            componentObjectId);
            assertEquals(expectedObjectName, componentBlobIdentifier.objectName);
        }
    }
}
