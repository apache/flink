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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Test {@link GSResumeRecoverable}. */
@ExtendWith(ParameterizedTestExtension.class)
public class GSResumeRecoverableTest {

    @Parameter public int position;

    @Parameter(value = 1)
    public boolean closed;

    @Parameter(value = 2)
    public List<UUID> componentObjectIds;

    @Parameter(value = 3)
    public String temporaryBucketName;

    @Parameters(name = "position={0}, closed={1}, componentObjectIds={2}, temporaryBucketName={3}")
    public static Collection<Object[]> data() {

        ArrayList<UUID> emptyComponentObjectIds = new ArrayList<>();
        ArrayList<UUID> populatedComponentObjectIds = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            populatedComponentObjectIds.add(UUID.randomUUID());
        }
        GSBlobIdentifier blobIdentifier = new GSBlobIdentifier("foo", "bar");

        return Arrays.asList(
                new Object[][] {
                    //  position=0, closed, no component ids, no explicit temporary bucket name
                    {0, true, emptyComponentObjectIds, null},
                    //  position=1024, not closed, no component ids, no explicit temporary bucket
                    // name
                    {1024, false, emptyComponentObjectIds, null},
                    //  position=0, closed, populated component ids, no explicit temporary bucket
                    // name
                    {0, true, populatedComponentObjectIds, null},
                    //  position=1024, not closed, populated component ids, no explicit temporary
                    // bucket name
                    {1024, false, populatedComponentObjectIds, null},
                    //  position=0, closed, populated component ids, explicit temporary bucket name
                    {0, true, populatedComponentObjectIds, "temporary-bucket"},
                    //  position=1024, not closed, populated component ids, explicit temporary
                    // bucket name
                    {1024, false, populatedComponentObjectIds, "temporary-bucket"},
                });
    }

    private GSBlobIdentifier blobIdentifier;

    @BeforeEach
    public void before() {
        blobIdentifier = new GSBlobIdentifier("foo", "bar");
    }

    @TestTemplate
    public void shouldConstructProperly() {
        GSResumeRecoverable resumeRecoverable =
                new GSResumeRecoverable(blobIdentifier, componentObjectIds, position, closed);
        assertEquals(blobIdentifier, resumeRecoverable.finalBlobIdentifier);
        assertEquals(position, resumeRecoverable.position);
        assertEquals(closed, resumeRecoverable.closed);
        assertEquals(componentObjectIds, resumeRecoverable.componentObjectIds);
    }

    /** Ensure that the list of component object ids cannot be added to. */
    @TestTemplate
    public void shouldNotAddComponentId() {
        GSResumeRecoverable resumeRecoverable =
                new GSResumeRecoverable(blobIdentifier, componentObjectIds, position, closed);

        assertThatThrownBy(() -> resumeRecoverable.componentObjectIds.add(UUID.randomUUID()))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    /** Ensure that component object ids can't be updated. */
    @TestTemplate
    public void shouldNotModifyComponentId() {
        GSResumeRecoverable resumeRecoverable =
                new GSResumeRecoverable(blobIdentifier, componentObjectIds, position, closed);

        assertThatThrownBy(() -> resumeRecoverable.componentObjectIds.set(0, UUID.randomUUID()))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
