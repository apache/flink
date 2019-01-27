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

package org.apache.flink.table.catalog.hive.config;

/**
 * Config for Hive tables.
 */
public class HiveTableConfig {
	// -------------------
	// Hive storage information configs
	// -------------------

	public static final String HIVE_TABLE_LOCATION = "hive.table.location";

	public static final String HIVE_TABLE_TYPE = "hive.table.type";

	public static final String HIVE_TABLE_SERDE_LIBRARY = "hive.table.serde.library";

	public static final String HIVE_TABLE_INPUT_FORMAT = "hive.table.input.format";

	public static final String HIVE_TABLE_OUTPUT_FORMAT = "hive.table.output.format";

	public static final String HIVE_TABLE_COMPRESSED = "hive.table.compressed";

	public static final String HIVE_TABLE_NUM_BUCKETS = "hive.table.num.buckets";

	public static final String HIVE_TABLE_STORAGE_SERIALIZATION_FORMAT = "hive.table.storage.serialization.format";

	public static final String HIVE_TABLE_FIELD_NAMES = "hive.table.field.names";

	public static final String HIVE_TABLE_FIELD_TYPES = "hive.table.field.types";

	public static final String HIVE_TABLE_DB_NAME = "hive.table.db.name";

	public static final String HIVE_TABLE_TABLE_NAME = "hive.table.table.name";

	public static final String HIVE_TABLE_PARTITION_FIELDS = "hive.table.partition.fields";

	// -------------------
	// Hive table parameters
	// -------------------

	public static final String HIVE_TABLE_PROPERTY_TRANSIENT_LASTDDLTIME = "transient_lastddltime";

	public static final String HIVE_TABLE_PROPERTY_LAST_MODIFIED_TIME = "last_modified_time";

	public static final String DEFAULT_LIST_COLUMN_TYPES_SEPARATOR = ":";
}
