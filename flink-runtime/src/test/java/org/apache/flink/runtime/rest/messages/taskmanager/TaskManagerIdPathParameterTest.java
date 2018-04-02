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

package org.apache.flink.runtime.rest.messages.taskmanager;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.testutils.category.New;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link TaskManagerIdPathParameter}.
 */
@Category(New.class)
public class TaskManagerIdPathParameterTest extends TestLogger {

	private TaskManagerIdPathParameter taskManagerIdPathParameter;

	@Before
	public void setUp() {
		taskManagerIdPathParameter = new TaskManagerIdPathParameter();
	}

	@Test
	public void testConversions() {
		final String resourceIdString = "foo";
		final ResourceID resourceId = taskManagerIdPathParameter.convertFromString(resourceIdString);
		assertThat(resourceId.getResourceIdString(), equalTo(resourceIdString));

		assertThat(taskManagerIdPathParameter.convertToString(resourceId), equalTo(resourceIdString));
	}

	@Test
	public void testIsMandatory() {
		assertTrue(taskManagerIdPathParameter.isMandatory());
	}

}
