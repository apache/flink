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

import org.apache.flink.formats.avro.generated.User;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests for the {@link Avro} descriptor.
 */
public class AvroTest extends DescriptorTestBase {
	@Override
	public List<Descriptor> descriptors() {
		final Descriptor desc1 = new Avro().recordClass(User.class);
		return Collections.singletonList(desc1);
	}

	@Override
	public List<Map<String, String>> properties() {
		final Map<String, String> props1 = new HashMap<>();
		props1.put(AvroValidator.FORMAT_AVRO_RECORD_CLASS, "org.apache.flink.formats.avro.generated.User");
		props1.put(AvroValidator.FORMAT_TYPE(), AvroValidator.FORMAT_TYPE_VALUE);
		props1.put(AvroValidator.FORMAT_PROPERTY_VERSION(), "1");

		return Collections.singletonList(props1);
	}

	@Override
	public DescriptorValidator validator() {
		return new AvroValidator();
	}

}
