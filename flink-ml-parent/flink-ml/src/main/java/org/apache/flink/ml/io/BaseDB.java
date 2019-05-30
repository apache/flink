/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.ml.io;

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.common.utils.RowTypeDataStream;
import org.apache.flink.ml.io.table.BaseDbTable;
import org.apache.flink.ml.params.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;

import java.io.Serializable;
import java.util.List;

/**
 * BaseDB is the base class of the concept which is similar with database.
 *
 * <p>Most of the meta system, such as mysql, hive etc, can be abstracted like a database.
 *
 * <p>- widely used operations in database
 *
 * <p>- execute sql-like query language
 *
 * <p>- direct read data from db, which is used in prediction processing for loading model.
 *
 * <p>example:
 *
 * <p><pre>{@code
 *     BaseDB db = BaseDB.of(new Params().set("ioName", "jdbc"))
 *     Table table = db.getBatchTable("tableName", new Params())
 * }
 * </pre>
 */
public abstract class BaseDB implements Serializable {

	protected Params params;

	protected BaseDB(Params params) {
		if (null == params) {
			this.params = new Params();
		} else {
			this.params = params.clone();
		}
		this.params.set(Constants.IO_NAME, AnnotationUtils.annotationName(this.getClass()));
	}

	public abstract List <String> listTableNames() throws Exception;

	public abstract boolean execute(String sql) throws Exception;

	public boolean createTable(String tableName, TableSchema schema) throws Exception {
		return createTable(tableName, schema, null);
	}

	public boolean createTable(String tableName, Params parameter) throws Exception {
		return createTable(tableName, getTableSchema(parameter), parameter);
	}

	public abstract boolean createTable(String tableName, TableSchema schema, Params parameter)
		throws Exception;

	public abstract boolean dropTable(String tableName) throws Exception;

	public abstract boolean hasTable(String table) throws Exception;

	public abstract boolean hasColumn(String tableName, String columnName) throws Exception;

	public abstract String[] getColNames(String tableName) throws Exception;

	public abstract TableSchema getTableSchema(String tableName) throws Exception;

	public TableSchema getTableSchema(Params parameter) {
		String tableSchmaStr = parameter.getString("tableSchema");
		if (tableSchmaStr == null || tableSchmaStr.length() == 0) {
			throw new RuntimeException("table schema is empty.");
		}

		String[] kvs = tableSchmaStr.split(",");
		String[] colNames = new String[kvs.length];
		TypeInformation <?>[] colTypes = new TypeInformation <?>[kvs.length];

		for (int i = 0; i < kvs.length; i++) {
			String[] splits = kvs[i].split(" ");
			if (splits.length != 2) {
				throw new RuntimeException("table schema error. " + tableSchmaStr);
			}
			colNames[i] = splits[0];
			switch (splits[1].trim().toLowerCase()) {
				case "string":
					colTypes[i] = Types.STRING;
					break;
				case "double":
					colTypes[i] = Types.DOUBLE;
					break;
				case "long":
					colTypes[i] = Types.LONG;
					break;
				case "boolean":
					colTypes[i] = Types.BOOLEAN;
					break;
				case "timestamp":
					colTypes[i] = Types.SQL_TIMESTAMP;
					break;
				default:
					break;
			}
		}

		return new TableSchema(colNames, colTypes);
	}

	public abstract void close() throws Exception;

	public abstract BaseDbTable getDbTable(String tableName) throws Exception;

	public abstract Table getStreamTable(String tableName, Params parameter) throws Exception;

	public abstract void sinkStream(String tableName, Table in, Params parameter);

	public void bucketingSinkStream(String tableName, Table in, Params parameter, TableSchema schema) {
		TableBucketingSink oof = new TableBucketingSink(tableName, parameter, schema, this);

		RowTypeDataStream.fromTable(in).addSink(oof);

	}

	public abstract Table getBatchTable(String tableName, Params parameter) throws Exception;

	public abstract void sinkBatch(String tableName, Table in, Params parameter);

	public abstract RichOutputFormat createFormat(String tableName, TableSchema schema);

	public abstract String getName();

	public Params getParams() {
		return this.params.clone();
	}

	public static BaseDB of(Params params) throws Exception {
		if (BaseDB.isDB(params)) {
			return AnnotationUtils.createDB(params.getString(Constants.IO_NAME), params);
		} else {
			throw new RuntimeException("NOT a DB parameter.");
		}
	}

	public static boolean isDB(Params params) {
		if (params.contains(Constants.IO_NAME)) {
			return AnnotationUtils.isDB(params.getString(Constants.IO_NAME));
		} else {
			return false;
		}
	}

}

