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

import org.apache.flink.connectors.hive.FlinkHiveException;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataDate;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
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
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.udf.generic.SimpleGenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.thrift.TException;

import javax.annotation.Nonnull;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
	public FileSinkOperator.RecordWriter getHiveRecordWriter(JobConf jobConf, Class outputFormatClz,
			Class<? extends Writable> outValClz, boolean isCompressed, Properties tableProps, Path outPath) {
		try {
			Class utilClass = HiveFileFormatUtils.class;
			HiveOutputFormat outputFormat = (HiveOutputFormat) outputFormatClz.newInstance();
			Method utilMethod = utilClass.getDeclaredMethod("getRecordWriter", JobConf.class, HiveOutputFormat.class,
					Class.class, boolean.class, Properties.class, Path.class, Reporter.class);
			return (FileSinkOperator.RecordWriter) utilMethod.invoke(null,
					jobConf, outputFormat, outValClz, isCompressed, tableProps, outPath, Reporter.NULL);
		} catch (Exception e) {
			throw new CatalogException("Failed to create Hive RecordWriter", e);
		}
	}

	@Override
	public Class getHiveOutputFormatClass(Class outputFormatClz) {
		try {
			Class utilClass = HiveFileFormatUtils.class;
			Method utilMethod = utilClass.getDeclaredMethod("getOutputFormatSubstitute", Class.class, boolean.class);
			Class res = (Class) utilMethod.invoke(null, outputFormatClz, false);
			Preconditions.checkState(res != null, "No Hive substitute output format for " + outputFormatClz);
			return res;
		} catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
			throw new FlinkHiveException("Failed to get HiveOutputFormat for " + outputFormatClz, e);
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

	@Override
	public Object toHiveTimestamp(Object flinkTimestamp) {
		if (flinkTimestamp == null) {
			return null;
		}
		ensureSupportedFlinkTimestamp(flinkTimestamp);
		return flinkTimestamp instanceof Timestamp ? flinkTimestamp : Timestamp.valueOf((LocalDateTime) flinkTimestamp);
	}

	@Override
	public LocalDateTime toFlinkTimestamp(Object hiveTimestamp) {
		Preconditions.checkArgument(hiveTimestamp instanceof Timestamp,
				"Expecting Hive timestamp to be an instance of %s, but actually got %s",
				Timestamp.class.getName(), hiveTimestamp.getClass().getName());
		return ((Timestamp) hiveTimestamp).toLocalDateTime();
	}

	@Override
	public Object toHiveDate(Object flinkDate) {
		if (flinkDate == null) {
			return null;
		}
		ensureSupportedFlinkDate(flinkDate);
		return flinkDate instanceof Date ? flinkDate : Date.valueOf((LocalDate) flinkDate);
	}

	@Override
	public LocalDate toFlinkDate(Object hiveDate) {
		Preconditions.checkArgument(hiveDate instanceof Date,
				"Expecting Hive Date to be an instance of %s, but actually got %s",
				Date.class.getName(), hiveDate.getClass().getName());
		return ((Date) hiveDate).toLocalDate();
	}

	@Override
	public Writable hivePrimitiveToWritable(Object value) {
		if (value == null) {
			return null;
		}
		Optional<Writable> optional = javaToWritable(value);
		return optional.orElseThrow(() -> new FlinkHiveException("Unsupported primitive java value of class " + value.getClass().getName()));
	}

	Optional<Writable> javaToWritable(@Nonnull Object value) {
		Writable writable = null;
		// in case value is already a Writable
		if (value instanceof Writable) {
			writable = (Writable) value;
		} else if (value instanceof Boolean) {
			writable = new BooleanWritable((Boolean) value);
		} else if (value instanceof Byte) {
			writable = new ByteWritable((Byte) value);
		} else if (value instanceof Short) {
			writable = new ShortWritable((Short) value);
		} else if (value instanceof Integer) {
			writable = new IntWritable((Integer) value);
		} else if (value instanceof Long) {
			writable = new LongWritable((Long) value);
		} else if (value instanceof Float) {
			writable = new FloatWritable((Float) value);
		} else if (value instanceof Double) {
			writable = new DoubleWritable((Double) value);
		} else if (value instanceof String) {
			writable = new Text((String) value);
		} else if (value instanceof HiveChar) {
			writable = new HiveCharWritable((HiveChar) value);
		} else if (value instanceof HiveVarchar) {
			writable = new HiveVarcharWritable((HiveVarchar) value);
		} else if (value instanceof HiveDecimal) {
			writable = new HiveDecimalWritable((HiveDecimal) value);
		} else if (value instanceof Date) {
			writable = new DateWritable((Date) value);
		} else if (value instanceof Timestamp) {
			writable = new TimestampWritable((Timestamp) value);
		} else if (value instanceof BigDecimal) {
			HiveDecimal hiveDecimal = HiveDecimal.create((BigDecimal) value);
			writable = new HiveDecimalWritable(hiveDecimal);
		} else if (value instanceof byte[]) {
			writable = new BytesWritable((byte[]) value);
		}
		return Optional.ofNullable(writable);
	}

	void ensureSupportedFlinkTimestamp(Object flinkTimestamp) {
		Preconditions.checkArgument(flinkTimestamp instanceof Timestamp || flinkTimestamp instanceof LocalDateTime,
				"Only support converting %s or %s to Hive timestamp, but not %s",
				Timestamp.class.getName(), LocalDateTime.class.getName(), flinkTimestamp.getClass().getName());
	}

	void ensureSupportedFlinkDate(Object flinkDate) {
		Preconditions.checkArgument(flinkDate instanceof Date || flinkDate instanceof LocalDate,
				"Only support converting %s or %s to Hive date, but not %s",
				Date.class.getName(), LocalDate.class.getName(), flinkDate.getClass().getName());
	}
}
