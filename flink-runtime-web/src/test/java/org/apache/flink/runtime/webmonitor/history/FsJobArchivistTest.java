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

import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.history.FsJobArchivist;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedJobGenerationUtils;
import org.apache.flink.runtime.webmonitor.WebRuntimeMonitor;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.regex.Pattern;

/**
 * This test must reside in flink-runtime-web since the {@link FsJobArchivist} relies on
 * {@link WebRuntimeMonitor#getJsonArchivists()}.
 */
public class FsJobArchivistTest {

	@Rule
	public TemporaryFolder tmpFolder = new TemporaryFolder();

	@Test
	public void testArchiveJob() throws Exception {
		Path tmpPath = new Path(tmpFolder.getRoot().getAbsolutePath());

		AccessExecutionGraph graph = ArchivedJobGenerationUtils.getTestJob();

		Collection<ArchivedJson> expected = new ArrayList<>();
		for (JsonArchivist archivist : WebRuntimeMonitor.getJsonArchivists()) {
			for (ArchivedJson archive : archivist.archiveJsonWithPath(graph)) {
				expected.add(archive);
			}
		}

		Path archivePath = FsJobArchivist.archiveJob(tmpPath, graph);
		Collection<ArchivedJson> actual = FsJobArchivist.getArchivedJsons(archivePath);

		Assert.assertEquals(expected.size(), actual.size());

		Iterator<ArchivedJson> eI = expected.iterator();
		Iterator<ArchivedJson> aI = actual.iterator();

		// several jsons contain a dynamic "now" field that depends on the time of creation, so we can't easily compare
		// the json and only check the path
		// /jobs/:jobid
		// /jobs/:jobid/vertices
		// /jobs/:jobid/vertices/:vertexid
		// /jobs/:jobid/vertices/:vertexid/subtasktimes
		// /jobs/:jobid/vertices/:vertexid/taskmanagers
		Pattern jobDetailsPattern = Pattern.compile("/jobs/[a-fA-F0-9]{32}(/vertices(/[a-fA-F0-9]{32}(/(subtasktimes|taskmanagers))?)?)?");
		while (eI.hasNext() && aI.hasNext()) {
			// technically there isn't guarantee that the order is identical, but as it stands this is the case
			ArchivedJson exp = eI.next();
			ArchivedJson act = aI.next();
			if (jobDetailsPattern.matcher(exp.getPath()).matches()) {
				Assert.assertEquals(exp.getPath(), act.getPath());
			} else {
				Assert.assertEquals(exp, act);
			}
		}
	}
}
