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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.internal.Registration;
import org.apache.flink.table.catalog.CatalogTableImpl;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Test for {@link ConnectTableDescriptor}.
 */
public class ConnectTableDescriptorTest {

	@Test
	public void testProperties() {
		AtomicReference<CatalogTableImpl> reference = new AtomicReference<>();
		Registration registration = (path, table) -> reference.set((CatalogTableImpl) table);
		ConnectTableDescriptor descriptor = new StreamTableDescriptor(
				registration, new FileSystem().path("myPath"))
				.withFormat(new FormatDescriptor("myFormat", 1) {
					@Override
					protected Map<String, String> toFormatProperties() {
						return new HashMap<>();
					}
				})
				.withSchema(new Schema()
						.field("f0", DataTypes.INT())
						.rowtime(new Rowtime().timestampsFromField("f0")));
		descriptor.createTemporaryTable("myTable");

		Assert.assertEquals(descriptor.toProperties(), reference.get().toProperties());
	}
}
