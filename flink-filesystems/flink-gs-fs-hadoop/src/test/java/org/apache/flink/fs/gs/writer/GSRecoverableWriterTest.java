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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

/** Test {@link GSRecoverableWriter}. */
@RunWith(Parameterized.class)
public class GSRecoverableWriterTest {

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
                    {emptyComponentObjectIds, null},
                    {emptyComponentObjectIds, "temporary-bucket"},
                    {populatedComponentObjectIds, null},
                    {populatedComponentObjectIds, "temporary-bucket"},
                });
    }

    @Test
    public void dummy() {}
}
