/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.module.hive;

import org.apache.flink.table.descriptors.ModuleDescriptor;
import org.apache.flink.table.factories.ModuleFactory;
import org.apache.flink.table.factories.TableFactoryService;
import org.apache.flink.table.module.Module;

import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;


/**
 * Test for {@link HiveModuleFactory}.
 */
public class HiveModuleFactoryTest {
	@Test
	public void test() {
		final String hiveVersion = "2.3.4";

		final HiveModule expected = new HiveModule(hiveVersion);

		final ModuleDescriptor moduleDescriptor = new HiveModuleDescriptor(hiveVersion);

		final Map<String, String> properties = moduleDescriptor.toProperties();

		final Module actualModule = TableFactoryService.find(ModuleFactory.class, properties)
			.createModule(properties);

		checkEquals(expected, (HiveModule) actualModule);
	}

	private static void checkEquals(HiveModule m1, HiveModule m2) {
		assertEquals(m1.getHiveVersion(), m2.getHiveVersion());
	}
}
