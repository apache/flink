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

package org.apache.flink.runtime.webmonitor.handlers;

import org.apache.flink.runtime.rest.messages.RestResponseMarshallingTestBase;

import java.util.ArrayList;
import java.util.List;

/**
 * Tests that the {@link ArtifactListInfo} can be marshalled and unmarshalled.
 */
public class ArtifactListInfoTest extends RestResponseMarshallingTestBase<ArtifactListInfo> {
	@Override
	protected Class<ArtifactListInfo> getTestResponseClass() {
		return ArtifactListInfo.class;
	}

	@Override
	protected ArtifactListInfo getTestResponseInstance() throws Exception {
		List<ArtifactListInfo.ArtifactEntryInfo> artifactEntryList1 = new ArrayList<>();
		artifactEntryList1.add(new ArtifactListInfo.ArtifactEntryInfo("name1", "desc1"));
		artifactEntryList1.add(new ArtifactListInfo.ArtifactEntryInfo("name2", "desc2"));

		List<ArtifactListInfo.ArtifactEntryInfo> artifactEntryList2 = new ArrayList<>();
		artifactEntryList2.add(new ArtifactListInfo.ArtifactEntryInfo("name3", "desc3"));
		artifactEntryList2.add(new ArtifactListInfo.ArtifactEntryInfo("name4", "desc4"));

		List<ArtifactListInfo.ArtifactFileInfo> artifactFileList = new ArrayList<>();
		artifactFileList.add(new ArtifactListInfo.ArtifactFileInfo("fileId1", "fileName1", System.currentTimeMillis(), artifactEntryList1));
		artifactFileList.add(new ArtifactListInfo.ArtifactFileInfo("fileId2", "fileName2", System.currentTimeMillis(), artifactEntryList2));

		return new ArtifactListInfo("local", artifactFileList);
	}
}
