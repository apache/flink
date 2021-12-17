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

package org.apache.flink.runtime.webmonitor.history;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.history.FsJobArchivist;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;
import java.util.Collection;

import static org.hamcrest.Matchers.containsInAnyOrder;

/** Tests for the {@link FsJobArchivist}. */
public class FsJobArchivistTest {

    @Rule public final TemporaryFolder tmpFolder = new TemporaryFolder();

    @Test
    public void testArchiveJob() throws Exception {
        final Path tmpPath = new Path(tmpFolder.getRoot().getAbsolutePath());
        final JobID jobId = new JobID();

        final Collection<ArchivedJson> toArchive = new ArrayList<>(2);
        toArchive.add(new ArchivedJson("dir1", "hello"));
        toArchive.add(new ArchivedJson("dir1/dir11", "world"));

        final Path archive = FsJobArchivist.archiveJob(tmpPath, jobId, toArchive);
        final Collection<ArchivedJson> restored = FsJobArchivist.getArchivedJsons(archive);

        Assert.assertThat(restored, containsInAnyOrder(toArchive.toArray()));
    }
}
