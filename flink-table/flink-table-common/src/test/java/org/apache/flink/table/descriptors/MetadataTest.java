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

package org.apache.flink.table.descriptors;

import org.apache.flink.table.api.ValidationException;

import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests for {@link Metadata}.
 */
public class MetadataTest extends DescriptorTestBase {

	@Test(expected = ValidationException.class)
	public void testInvalidCreationTime() {
		addPropertyAndVerify(descriptors().get(0), "metadata.creation-time", "dfghj");
	}

	// ----------------------------------------------------------------------------------------------

	@Override
	public List<Descriptor> descriptors() {
		Metadata desc = new Metadata()
			.comment("Some additional comment")
			.creationTime(123L)
			.lastAccessTime(12020202L);

		return Arrays.asList(desc);
	}

	@Override
	public DescriptorValidator validator() {
		return new MetadataValidator();
	}

	@Override
	public List<Map<String, String>> properties() {
		Map props = new HashMap<String, String>() {
			{
				put("metadata.comment", "Some additional comment");
				put("metadata.creation-time", "123");
				put("metadata.last-access-time", "12020202");
			}
		};

		return Arrays.asList(props);
	}
}
