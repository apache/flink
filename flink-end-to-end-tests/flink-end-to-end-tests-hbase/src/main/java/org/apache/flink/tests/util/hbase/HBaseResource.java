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

package org.apache.flink.tests.util.hbase;

import org.apache.flink.tests.util.util.FactoryUtils;
import org.apache.flink.util.ExternalResource;

import java.io.IOException;
import java.util.List;

/**
 * Generic interface for interacting with HBase.
 */
public interface HBaseResource extends ExternalResource {

	/**
	 * Creates a table with the given name and column families.
	 *
	 * @param tableName      desired table name
	 * @param columnFamilies column family to create
	 * @throws IOException
	 */
	void createTable(String tableName, String... columnFamilies) throws IOException;

	/**
	 * Scan the given HBase table.
	 *
	 * @param tableName table desired to scan
	 * @throws IOException
	 */
	List<String> scanTable(String tableName) throws IOException;

	/**
	 * Put the given data to the given table.
	 *
	 * @param tableName       table to put data
	 * @param rowKey          row key of the given data
	 * @param columnFamily    column family of the given data
	 * @param columnQualifier  column qualifier of the given data
	 * @param value           value of the given data
	 * @throws IOException
	 */
	void putData(String tableName, String rowKey, String columnFamily, String columnQualifier, String value) throws IOException;

	/**
	 * Returns the configured HBaseResource implementation, or a {@link LocalStandaloneHBaseResource} if none is configured.
	 *
	 * @return configured HbaseResource, or {@link LocalStandaloneHBaseResource} if none is configured
	 */
	static HBaseResource get() {
		return FactoryUtils.loadAndInvokeFactory(
			HBaseResourceFactory.class,
			HBaseResourceFactory::create,
			LocalStandaloneHBaseResourceFactory::new);
	}
}
