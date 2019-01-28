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

package org.apache.flink.addons.hbase;

import org.apache.hadoop.hbase.client.Scan;

/**
 * Interface shared by {@link TableInputFormat} and {@link TableSnapshotInputFormat},
 * subclass of the table input format should override methods defined in this interface.
 */
public interface HBaseTableScannerAware {

	/**
	 * Returns an instance of Scan that retrieves the required subset of records from the HBase table.
	 *
	 * @return The appropriate instance of Scan for this usecase.
	 */
	Scan getScanner();

	/**
	 * What table is to be read.
	 * Per instance of a TableInputFormat derivative only a single tablename is possible.
	 *
	 * @return The name of the table
	 */
	String getTableName();
}
