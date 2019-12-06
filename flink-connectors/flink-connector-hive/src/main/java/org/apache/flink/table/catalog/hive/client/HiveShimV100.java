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
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.hive.util.HiveReflectionUtils;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataDate;
import org.apache.flink.table.functions.hive.FlinkHiveUDFException;
import org.apache.flink.table.functions.hive.conversion.HiveInspectors;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.HiveStatsUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.udf.generic.SimpleGenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantBinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantBooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantDateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantDoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantFloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantHiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantHiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantHiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantIntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantLongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantTimestampObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.thrift.TException;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

/**
 * Shim for Hive version 1.0.0.
 */
public class HiveShimV100 implements HiveShim {

	@Override
	public IMetaStoreClient getHiveMetastoreClient(HiveConf hiveConf) {
		try {
			return new HiveMetaStoreClient(hiveConf);
		} catch (MetaException ex) {
			throw new CatalogException("Failed to create Hive Metastore client", ex);
		}
	}

	@Override
	// 1.x client doesn't support filtering tables by type, so here we need to get all tables and filter by ourselves
	public List<String> getViews(IMetaStoreClient client, String databaseName) throws UnknownDBException, TException {
		// We don't have to use reflection here because client.getAllTables(String) is supposed to be there for
		// all versions.
		List<String> tableNames = client.getAllTables(databaseName);
		List<String> views = new ArrayList<>();
		for (String name : tableNames) {
			Table table = client.getTable(databaseName, name);
			String viewDef = table.getViewOriginalText();
			if (viewDef != null && !viewDef.isEmpty()) {
				views.add(table.getTableName());
			}
		}
		return views;
	}

	@Override
	public boolean moveToTrash(FileSystem fs, Path path, Configuration conf, boolean purge) throws IOException {
		try {
			Method method = FileUtils.class.getDeclaredMethod("moveToTrash", FileSystem.class, Path.class, Configuration.class);
			return (boolean) method.invoke(null, fs, path, conf);
		} catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
			throw new IOException("Failed to move " + path + " to trash", e);
		}
	}

	@Override
	public void alterTable(IMetaStoreClient client, String databaseName, String tableName, Table table) throws InvalidOperationException, MetaException, TException {
		client.alter_table(databaseName, tableName, table);
	}

	@Override
	public void alterPartition(IMetaStoreClient client, String databaseName, String tableName, Partition partition)
			throws InvalidOperationException, MetaException, TException {
		String errorMsg = "Failed to alter partition for table %s in database %s";
		try {
			Method method = client.getClass().getMethod("alter_partition", String.class, String.class, Partition.class);
			method.invoke(client, databaseName, tableName, partition);
		} catch (InvocationTargetException ite) {
			Throwable targetEx = ite.getTargetException();
			if (targetEx instanceof TException) {
				throw (TException) targetEx;
			} else {
				throw new CatalogException(String.format(errorMsg, tableName, databaseName), targetEx);
			}
		} catch (NoSuchMethodException | IllegalAccessException e) {
			throw new CatalogException(String.format(errorMsg, tableName, databaseName), e);
		}
	}

	@Override
	public SimpleGenericUDAFParameterInfo createUDAFParameterInfo(ObjectInspector[] params, boolean isWindowing, boolean distinct, boolean allColumns) {
		try {
			Constructor constructor = SimpleGenericUDAFParameterInfo.class.getConstructor(ObjectInspector[].class,
					boolean.class, boolean.class);
			return (SimpleGenericUDAFParameterInfo) constructor.newInstance(params, distinct, allColumns);
		} catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
			throw new CatalogException("Failed to create SimpleGenericUDAFParameterInfo", e);
		}
	}

	@Override
	public Class<?> getMetaStoreUtilsClass() {
		try {
			return Class.forName("org.apache.hadoop.hive.metastore.MetaStoreUtils");
		} catch (ClassNotFoundException e) {
			throw new CatalogException("Failed to find class MetaStoreUtils", e);
		}
	}

	@Override
	public Class<?> getHiveMetaStoreUtilsClass() {
		return getMetaStoreUtilsClass();
	}

	@Override
	public Class<?> getDateDataTypeClass() {
		return java.sql.Date.class;
	}

	@Override
	public Class<?> getTimestampDataTypeClass() {
		return java.sql.Timestamp.class;
	}

	@Override
	public FileStatus[] getFileStatusRecurse(Path path, int level, FileSystem fs) throws IOException {
		try {
			Method method = HiveStatsUtils.class.getMethod("getFileStatusRecurse", Path.class, Integer.TYPE, FileSystem.class);
			// getFileStatusRecurse is a static method
			return (FileStatus[]) method.invoke(null, path, level, fs);
		} catch (Exception ex) {
			throw new CatalogException("Failed to invoke HiveStatsUtils.getFileStatusRecurse()", ex);
		}
	}

	@Override
	public void makeSpecFromName(Map<String, String> partSpec, Path currPath) {
		try {
			Method method = Warehouse.class.getMethod("makeSpecFromName", Map.class, Path.class);
			// makeSpecFromName is a static method
			method.invoke(null, partSpec, currPath);
		} catch (Exception ex) {
			throw new CatalogException("Failed to invoke Warehouse.makeSpecFromName()", ex);
		}
	}

	@Override
	public ObjectInspector getObjectInspectorForConstant(PrimitiveTypeInfo primitiveTypeInfo, Object value) {
		String className;
		value = HiveInspectors.hivePrimitiveToWritable(value);
		// Java constant object inspectors are not available until 1.2.0 -- https://issues.apache.org/jira/browse/HIVE-9766
		// So we have to use writable constant object inspectors for 1.1.x
		switch (primitiveTypeInfo.getPrimitiveCategory()) {
			case BOOLEAN:
				className = WritableConstantBooleanObjectInspector.class.getName();
				return HiveReflectionUtils.createConstantObjectInspector(className, value);
			case BYTE:
				className = WritableConstantByteObjectInspector.class.getName();
				return HiveReflectionUtils.createConstantObjectInspector(className, value);
			case SHORT:
				className = WritableConstantShortObjectInspector.class.getName();
				return HiveReflectionUtils.createConstantObjectInspector(className, value);
			case INT:
				className = WritableConstantIntObjectInspector.class.getName();
				return HiveReflectionUtils.createConstantObjectInspector(className, value);
			case LONG:
				className = WritableConstantLongObjectInspector.class.getName();
				return HiveReflectionUtils.createConstantObjectInspector(className, value);
			case FLOAT:
				className = WritableConstantFloatObjectInspector.class.getName();
				return HiveReflectionUtils.createConstantObjectInspector(className, value);
			case DOUBLE:
				className = WritableConstantDoubleObjectInspector.class.getName();
				return HiveReflectionUtils.createConstantObjectInspector(className, value);
			case STRING:
				className = WritableConstantStringObjectInspector.class.getName();
				return HiveReflectionUtils.createConstantObjectInspector(className, value);
			case CHAR:
				className = WritableConstantHiveCharObjectInspector.class.getName();
				try {
					return (ObjectInspector) Class.forName(className).getDeclaredConstructor(
							CharTypeInfo.class, value.getClass()).newInstance(primitiveTypeInfo, value);
				} catch (Exception e) {
					throw new FlinkHiveUDFException("Failed to create writable constant object inspector", e);
				}
			case VARCHAR:
				className = WritableConstantHiveVarcharObjectInspector.class.getName();
				try {
					return (ObjectInspector) Class.forName(className).getDeclaredConstructor(
							VarcharTypeInfo.class, value.getClass()).newInstance(primitiveTypeInfo, value);
				} catch (Exception e) {
					throw new FlinkHiveUDFException("Failed to create writable constant object inspector", e);
				}
			case DATE:
				className = WritableConstantDateObjectInspector.class.getName();
				return HiveReflectionUtils.createConstantObjectInspector(className, value);
			case TIMESTAMP:
				className = WritableConstantTimestampObjectInspector.class.getName();
				return HiveReflectionUtils.createConstantObjectInspector(className, value);
			case DECIMAL:
				className = WritableConstantHiveDecimalObjectInspector.class.getName();
				return HiveReflectionUtils.createConstantObjectInspector(className, value);
			case BINARY:
				className = WritableConstantBinaryObjectInspector.class.getName();
				return HiveReflectionUtils.createConstantObjectInspector(className, value);
			case UNKNOWN:
			case VOID:
				// If type is null, we use the Constant String to replace
				className = WritableConstantStringObjectInspector.class.getName();
				return HiveReflectionUtils.createConstantObjectInspector(className, value.toString());
			default:
				throw new FlinkHiveUDFException(
						String.format("Cannot find ConstantObjectInspector for %s", primitiveTypeInfo));
		}
	}

	@Override
	public ColumnStatisticsData toHiveDateColStats(CatalogColumnStatisticsDataDate flinkDateColStats) {
		throw new UnsupportedOperationException("DATE column stats are not supported until Hive 1.2.0");
	}

	@Override
	public boolean isDateStats(ColumnStatisticsData colStatsData) {
		return false;
	}

	@Override
	public CatalogColumnStatisticsDataDate toFlinkDateColStats(ColumnStatisticsData hiveDateColStats) {
		throw new UnsupportedOperationException("DATE column stats are not supported until Hive 1.2.0");
	}

	@Override
	public FileSinkOperator.RecordWriter getHiveRecordWriter(JobConf jobConf, String outputFormatClzName,
			Class<? extends Writable> outValClz, boolean isCompressed, Properties tableProps, Path outPath) {
		try {
			Class outputFormatClz = Class.forName(outputFormatClzName);
			Class utilClass = HiveFileFormatUtils.class;
			Method utilMethod = utilClass.getDeclaredMethod("getOutputFormatSubstitute", Class.class, boolean.class);
			outputFormatClz = (Class) utilMethod.invoke(null, outputFormatClz, false);
			Preconditions.checkState(outputFormatClz != null, "No Hive substitute output format for " + outputFormatClzName);
			HiveOutputFormat outputFormat = (HiveOutputFormat) outputFormatClz.newInstance();
			utilMethod = utilClass.getDeclaredMethod("getRecordWriter", JobConf.class, HiveOutputFormat.class,
					Class.class, boolean.class, Properties.class, Path.class, Reporter.class);
			return (FileSinkOperator.RecordWriter) utilMethod.invoke(null,
					jobConf, outputFormat, outValClz, isCompressed, tableProps, outPath, Reporter.NULL);
		} catch (Exception e) {
			throw new CatalogException("Failed to create Hive RecordWriter", e);
		}
	}

	@Override
	public List<FieldSchema> getFieldsFromDeserializer(Configuration conf, Table table, boolean skipConfError) {
		try {
			Method utilMethod = getHiveMetaStoreUtilsClass().getMethod("getDeserializer", Configuration.class, Table.class);
			Deserializer deserializer = (Deserializer) utilMethod.invoke(null, conf, table);
			utilMethod = getHiveMetaStoreUtilsClass().getMethod("getFieldsFromDeserializer", String.class, Deserializer.class);
			return (List<FieldSchema>) utilMethod.invoke(null, table.getTableName(), deserializer);
		} catch (Exception e) {
			throw new CatalogException("Failed to get table schema from deserializer", e);
		}
	}

	@Override
	public Set<String> listBuiltInFunctions() {
		// FunctionInfo doesn't have isBuiltIn() API to tell whether it's a builtin function or not
		// prior to Hive 1.2.0
		throw new UnsupportedOperationException("Listing built in functions are not supported until Hive 1.2.0");
	}

	@Override
	public Optional<FunctionInfo> getBuiltInFunctionInfo(String name) {
		// FunctionInfo doesn't have isBuiltIn() API to tell whether it's a builtin function or not
		// prior to Hive 1.2.0
		throw new UnsupportedOperationException("Getting built in functions are not supported until Hive 1.2.0");
	}

	@Override
	public Set<String> getNotNullColumns(IMetaStoreClient client, Configuration conf, String dbName, String tableName) {
		// NOT NULL constraints not supported until 3.0.0 -- HIVE-16575
		return Collections.emptySet();
	}

	@Override
	public Optional<UniqueConstraint> getPrimaryKey(IMetaStoreClient client, String dbName, String tableName, byte requiredTrait) {
		// PK constraints not supported until 2.1.0 -- HIVE-13290
		return Optional.empty();
	}
}
