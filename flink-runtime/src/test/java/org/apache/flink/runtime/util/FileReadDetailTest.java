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

package org.apache.flink.runtime.util;

import org.apache.flink.runtime.clusterframework.types.ResourceID;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class FileReadDetailTest {

	final ResourceID trId1 = ResourceID.generate();
	final FileReadDetail fo1 = new FileReadDetail(trId1, "taskmanager.log", 10L, 20L);
	final FileReadDetail fo2 = new FileReadDetail(trId1, "taskmanager.log", 10L, 30L);
	final FileReadDetail fo3 = new FileReadDetail(trId1, "taskmanager.log", 10L, null);
	final FileReadDetail fo4 = new FileReadDetail(trId1, "taskmanager.log", null, null);
	final FileReadDetail fo5 = new FileReadDetail(trId1, "taskmanager-1.log", 10L, 20L);
	final FileReadDetail fo6 = new FileReadDetail(ResourceID.generate(), "taskmanager.log", 10L, 30L);
	final FileReadDetail fo7 = new FileReadDetail(trId1, "taskmanager.log", 10L, 20L);
	final FileReadDetail fo8 = new FileReadDetail(trId1, "taskmanager.log", 10L, null);
	final FileReadDetail fo9 = new FileReadDetail(trId1, "taskmanager.log", null, null);

	@Test
	public void testHashCode(){
		assertTrue(fo1.hashCode() != fo2.hashCode());
		assertTrue(fo1.hashCode() != fo3.hashCode());
		assertTrue(fo1.hashCode() != fo4.hashCode());
		assertTrue(fo1.hashCode() != fo5.hashCode());
		assertTrue(fo1.hashCode() != fo6.hashCode());
		assertTrue(fo1.hashCode() == fo7.hashCode());
	}

	@Test
	public void testEqual(){
		assertNotEquals(fo1, fo2);
		assertNotEquals(fo1, fo3);
		assertNotEquals(fo1, fo4);
		assertNotEquals(fo1, fo5);
		assertNotEquals(fo1, fo6);
		assertEquals(fo1, fo7);
		assertEquals(fo3, fo8);
		assertEquals(fo4, fo9);
	}
}
