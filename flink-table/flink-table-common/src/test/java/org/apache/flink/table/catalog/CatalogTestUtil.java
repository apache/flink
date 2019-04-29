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

package org.apache.flink.table.catalog;

import static org.junit.Assert.assertEquals;

/**
 * Utility class for catalog testing.
 */
public class CatalogTestUtil {

	public static void checkEquals(CatalogTable t1, CatalogTable t2) {
		assertEquals(t1.getSchema(), t2.getSchema());
		assertEquals(t1.getComment(), t2.getComment());
		assertEquals(t1.getProperties(), t2.getProperties());
		assertEquals(t1.getPartitionKeys(), t2.getPartitionKeys());
		assertEquals(t1.isPartitioned(), t2.isPartitioned());
		assertEquals(t1.getDescription(), t2.getDescription());
	}

	public static void checkEquals(CatalogView v1, CatalogView v2) {
		assertEquals(v1.getSchema(), v1.getSchema());
		assertEquals(v1.getProperties(), v2.getProperties());
		assertEquals(v1.getComment(), v2.getComment());
		assertEquals(v1.getOriginalQuery(), v2.getOriginalQuery());
		assertEquals(v1.getExpandedQuery(), v2.getExpandedQuery());
	}

	public static void checkEquals(CatalogDatabase d1, CatalogDatabase d2) {
		assertEquals(d1.getProperties(), d2.getProperties());
	}

	public static void checkEquals(CatalogFunction f1, CatalogFunction f2) {
		assertEquals(f1.getClassName(), f2.getClassName());
		assertEquals(f1.getProperties(), f2.getProperties());
	}

	public static void checkEquals(CatalogPartition p1, CatalogPartition p2) {
		assertEquals(p1.getProperties(), p2.getProperties());
	}

	static void checkEquals(CatalogTableStatistics ts1, CatalogTableStatistics ts2) {
		assertEquals(ts1.getRowCount(), ts2.getRowCount());
		assertEquals(ts1.getFileCount(), ts2.getFileCount());
		assertEquals(ts1.getTotalSize(), ts2.getTotalSize());
		assertEquals(ts1.getRawDataSize(), ts2.getRawDataSize());
		assertEquals(ts1.getProperties(), ts2.getProperties());
	}

	static void checkEquals(CatalogColumnStatistics cs1, CatalogColumnStatistics cs2) {
		assertEquals(cs1.getNdv(), cs2.getNdv());
		assertEquals(cs1.getNullCount(), cs2.getNullCount());
		assertEquals(cs1.getAvgLen(), cs2.getAvgLen());
		assertEquals(cs1.getMaxLen(), cs2.getMaxLen());
		assertEquals(cs1.getMinValue(), cs2.getMinValue());
		assertEquals(cs2.getMaxValue(), cs2.getMaxValue());
		assertEquals(cs1.getProperties(), cs2.getProperties());
	}
}
