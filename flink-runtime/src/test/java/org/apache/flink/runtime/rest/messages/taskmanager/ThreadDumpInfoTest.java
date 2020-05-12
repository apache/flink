/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rest.messages.taskmanager;

import org.apache.flink.runtime.rest.messages.RestResponseMarshallingTestBase;

import java.util.Arrays;
import java.util.Collection;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;

/**
 * Test for (un)marshalling of the {@link ThreadDumpInfo}.
 */
public class ThreadDumpInfoTest extends RestResponseMarshallingTestBase<ThreadDumpInfo> {

	@Override
	protected Class<ThreadDumpInfo> getTestResponseClass() {
		return ThreadDumpInfo.class;
	}

	@Override
	protected ThreadDumpInfo getTestResponseInstance() throws Exception {
		final Collection<ThreadDumpInfo.ThreadInfo> threadInfos = Arrays.asList(
			ThreadDumpInfo.ThreadInfo.create("foobar", "barfoo"),
			ThreadDumpInfo.ThreadInfo.create("bar", "foo"));

		return ThreadDumpInfo.create(threadInfos);
	}

	@Override
	protected void assertOriginalEqualsToUnmarshalled(ThreadDumpInfo expected, ThreadDumpInfo actual) {
		assertThat(actual.getThreadInfos(), containsInAnyOrder(expected.getThreadInfos().toArray()));
	}
}
