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

package org.apache.flink.table.catalog.hive.client;

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataDate;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.udf.generic.SimpleGenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.thrift.TException;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

/**
 * A shim layer to support different versions of Hive.
 */
public interface HiveShim extends Serializable {

	/**
	 * Create a Hive Metastore client based on the given HiveConf object.
	 *
	 * @param hiveConf HiveConf instance
	 * @return an IMetaStoreClient instance
	 */
	IMetaStoreClient getHiveMetastoreClient(HiveConf hiveConf);

	/**
	 * Get a list of views in the given database from the given Hive Metastore client.
	 *
	 * @param client       Hive Metastore client
	 * @param databaseName the name of the database
	 * @return A list of names of the views
	 * @throws UnknownDBException if the database doesn't exist
	 * @throws TException         for any other generic exceptions caused by Thrift
	 */
	List<String> getViews(IMetaStoreClient client, String databaseName) throws UnknownDBException, TException;

	/**
	 * Alters a Hive table.
	 *
	 * @param client       the Hive metastore client
	 * @param databaseName the name of the database to which the table belongs
	 * @param tableName    the name of the table to be altered
	 * @param table        the new Hive table
	 */
	void alterTable(IMetaStoreClient client, String databaseName, String tableName, Table table)
			throws InvalidOperationException, MetaException, TException;

	void alterPartition(IMetaStoreClient client, String databaseName, String tableName, Partition partition)
			throws InvalidOperationException, MetaException, TException;

	/**
	 * Creates SimpleGenericUDAFParameterInfo.
	 */
	SimpleGenericUDAFParameterInfo createUDAFParameterInfo(ObjectInspector[] params, boolean isWindowing,
			boolean distinct, boolean allColumns);

	/**
	 * Get the class of Hive's MetaStoreUtils because its package name was changed in Hive 3.1.0.
	 *
	 * @return MetaStoreUtils class
	 */
	Class<?> getMetaStoreUtilsClass();

	/**
	 * Get the class of Hive's HiveMetaStoreUtils as it was split from MetaStoreUtils class in Hive 3.1.0.
	 *
	 * @return HiveMetaStoreUtils class
	 */
	Class<?> getHiveMetaStoreUtilsClass();

	/**
	 * Hive Date data type class was changed in Hive 3.1.0.
	 *
	 * @return Hive's Date class
	 */
	Class<?> getDateDataTypeClass();

	/**
	 * Hive Timestamp data type class was changed in Hive 3.1.0.
	 *
	 * @return Hive's Timestamp class
	 */
	Class<?> getTimestampDataTypeClass();

	/**
	 * Generate Hive ColumnStatisticsData from Flink CatalogColumnStatisticsDataDate for DATE columns.
	 */
	ColumnStatisticsData toHiveDateColStats(CatalogColumnStatisticsDataDate flinkDateColStats);

	/**
	 * Whether a Hive ColumnStatisticsData is for DATE columns.
	 */
	boolean isDateStats(ColumnStatisticsData colStatsData);

	/**
	 * Generate Flink CatalogColumnStatisticsDataDate from Hive ColumnStatisticsData for DATE columns.
	 */
	CatalogColumnStatisticsDataDate toFlinkDateColStats(ColumnStatisticsData hiveDateColStats);

	/**
	 * Get Hive's FileSinkOperator.RecordWriter.
	 */
	FileSinkOperator.RecordWriter getHiveRecordWriter(JobConf jobConf, Class outputFormatClz,
			Class<? extends Writable> outValClz, boolean isCompressed, Properties tableProps, Path outPath);

	/**
	 * For a given OutputFormat class, get the corresponding {@link HiveOutputFormat} class.
	 */
	Class getHiveOutputFormatClass(Class outputFormatClz);

	/**
	 * Get Hive table schema from deserializer.
	 */
	List<FieldSchema> getFieldsFromDeserializer(Configuration conf, Table table, boolean skipConfError);

	/**
	 * List names of all built-in functions.
	 */
	Set<String> listBuiltInFunctions();

	/**
	 * Get a Hive built-in function by name.
	 */
	Optional<FunctionInfo> getBuiltInFunctionInfo(String name);

	/**
	 * Get the set of columns that have NOT NULL constraints.
	 */
	Set<String> getNotNullColumns(IMetaStoreClient client, Configuration conf, String dbName, String tableName);

	/**
	 * Get the primary key of a Hive table and convert it to a UniqueConstraint. Return empty if the table
	 * doesn't have a primary key, or the constraint doesn't satisfy the desired trait, e.g. RELY.
	 */
	Optional<UniqueConstraint> getPrimaryKey(IMetaStoreClient client, String dbName, String tableName, byte requiredTrait);

	/**
	 * Converts a Flink timestamp instance to what's expected by Hive.
	 */
	@Nullable Object toHiveTimestamp(@Nullable Object flinkTimestamp);

	/**
	 * Converts a hive timestamp instance to LocalDateTime which is expected by DataFormatConverter.
	 */
	LocalDateTime toFlinkTimestamp(Object hiveTimestamp);

	/**
	 * Converts a Flink date instance to what's expected by Hive.
	 */
	@Nullable Object toHiveDate(@Nullable Object flinkDate);

	/**
	 * Converts a hive date instance to LocalDate which is expected by DataFormatConverter.
	 */
	LocalDate toFlinkDate(Object hiveDate);

	/**
	 * Converts a Hive primitive java object to corresponding Writable object.
	 */
	@Nullable Writable hivePrimitiveToWritable(@Nullable Object value);

	/**
	 * Creates a table with PK and NOT NULL constraints.
	 */
	void createTableWithConstraints(IMetaStoreClient client, Table table, Configuration conf,
			UniqueConstraint pk, List<Byte> pkTraits, List<String> notNullCols, List<Byte> nnTraits);

	/**
	 * Create orc {@link BulkWriter.Factory} for different hive versions.
	 */
	BulkWriter.Factory<RowData> createOrcBulkWriterFactory(
			Configuration conf, String schema, LogicalType[] fieldTypes);
}
