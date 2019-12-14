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

package org.apache.flink.table.catalog.hive;

import org.apache.flink.table.catalog.CatalogPartition;

import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test for HiveCatalog on generic metadata.
 */
public class HiveCatalogGenericMetadataTest extends HiveCatalogMetadataTestBase {

	@BeforeClass
	public static void init() {
		catalog = HiveTestUtils.createHiveCatalog();
		catalog.open();
	}

	// ------ partitions ------

	@Test
	public void testCreatePartition() throws Exception {
	}

	@Test
	public void testCreatePartition_TableNotExistException() throws Exception {
	}

	@Test
	public void testCreatePartition_TableNotPartitionedException() throws Exception {
	}

	@Test
	public void testCreatePartition_PartitionSpecInvalidException() throws Exception {
	}

	@Test
	public void testCreatePartition_PartitionAlreadyExistsException() throws Exception {
	}

	@Test
	public void testCreatePartition_PartitionAlreadyExists_ignored() throws Exception {
	}

	@Test
	public void testDropPartition() throws Exception {
	}

	@Test
	public void testDropPartition_TableNotExist() throws Exception {
	}

	@Test
	public void testDropPartition_TableNotPartitioned() throws Exception {
	}

	@Test
	public void testDropPartition_PartitionSpecInvalid() throws Exception {
	}

	@Test
	public void testDropPartition_PartitionNotExist() throws Exception {
	}

	@Test
	public void testDropPartition_PartitionNotExist_ignored() throws Exception {
	}

	@Test
	public void testAlterPartition() throws Exception {
	}

	@Test
	public void testAlterPartition_TableNotExist() throws Exception {
	}

	@Test
	public void testAlterPartition_TableNotPartitioned() throws Exception {
	}

	@Test
	public void testAlterPartition_PartitionSpecInvalid() throws Exception {
	}

	@Test
	public void testAlterPartition_PartitionNotExist() throws Exception {
	}

	@Test
	public void testAlterPartition_PartitionNotExist_ignored() throws Exception {
	}

	@Test
	public void testGetPartition_TableNotExist() throws Exception {
	}

	@Test
	public void testGetPartition_TableNotPartitioned() throws Exception {
	}

	@Test
	public void testGetPartition_PartitionSpecInvalid_invalidPartitionSpec() throws Exception {
	}

	@Test
	public void testGetPartition_PartitionSpecInvalid_sizeNotEqual() throws Exception {
	}

	@Test
	public void testGetPartition_PartitionNotExist() throws Exception {
	}

	@Test
	public void testPartitionExists() throws Exception {
	}

	@Test
	public void testListPartitionPartialSpec() throws Exception {
	}

	@Override
	public void testGetPartitionStats() throws Exception {
	}

	@Override
	public void testAlterPartitionTableStats() throws Exception {
	}

	// ------ test utils ------

	@Override
	protected boolean isGeneric() {
		return true;
	}

	@Override
	public CatalogPartition createPartition() {
		throw new UnsupportedOperationException();
	}
}
