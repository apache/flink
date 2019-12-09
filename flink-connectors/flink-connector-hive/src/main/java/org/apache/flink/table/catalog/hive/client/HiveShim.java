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

import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataDate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
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
import org.apache.hadoop.hive.ql.udf.generic.SimpleGenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.thrift.TException;

import java.io.IOException;
import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
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
	 * Moves a particular file or directory to trash.
	 * The file/directory can potentially be deleted (w/o going to trash) if purge is set to true, or if it cannot
	 * be moved properly.
	 *
	 * <p>This interface is here because FileUtils.moveToTrash in different Hive versions have different signatures.
	 *
	 * @param fs    the FileSystem to use
	 * @param path  the path of the file or directory to be moved to trash.
	 * @param conf  the Configuration to use
	 * @param purge whether try to skip trash and directly delete the file/directory. This flag may be ignored by
	 *              old Hive versions prior to 2.3.0.
	 * @return true if the move is successful, and false otherwise
	 * @throws IOException if the file/directory cannot be properly moved or deleted
	 */
	boolean moveToTrash(FileSystem fs, Path path, Configuration conf, boolean purge) throws IOException;

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
	 * The return type of HiveStatsUtils.getFileStatusRecurse was changed from array to List in Hive 3.1.0.
	 *
	 * @param path the path of the directory
	 * @param level the level of recursion
	 * @param fs the file system of the directory
	 * @return an array of the entries
	 * @throws IOException in case of any io error
	 */
	FileStatus[] getFileStatusRecurse(Path path, int level, FileSystem fs) throws IOException;

	/**
	 * The signature of HiveStatsUtils.makeSpecFromName() was changed in Hive 3.1.0.
	 *
	 * @param partSpec partition specs
	 * @param currPath the current path
	 */
	void makeSpecFromName(Map<String, String> partSpec, Path currPath);

	/**
	 * Get ObjectInspector for a constant value.
	 */
	ObjectInspector getObjectInspectorForConstant(PrimitiveTypeInfo primitiveTypeInfo, Object value);

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
	FileSinkOperator.RecordWriter getHiveRecordWriter(JobConf jobConf, String outputFormatClzName,
			Class<? extends Writable> outValClz, boolean isCompressed, Properties tableProps, Path outPath);

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
	Object toHiveTimestamp(Object flinkTimestamp);

	/**
	 * Converts a hive timestamp instance to LocalDateTime which is expected by DataFormatConverter.
	 */
	LocalDateTime toFlinkTimestamp(Object hiveTimestamp);

	/**
	 * Converts a Flink date instance to what's expected by Hive.
	 */
	Object toHiveDate(Object flinkDate);

	/**
	 * Converts a hive date instance to LocalDate which is expected by DataFormatConverter.
	 */
	LocalDate toFlinkDate(Object hiveDate);
}
