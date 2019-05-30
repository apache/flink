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

package org.apache.flink.ml.batchoperator.sink;

import org.apache.flink.ml.batchoperator.BatchOperator;
import org.apache.flink.ml.io.AnnotationUtils;
import org.apache.flink.ml.io.BaseDB;
import org.apache.flink.ml.params.Params;
import org.apache.flink.table.api.TableSchema;

/**
 * Batch sink for the DataBase.
 */
public final class DBSinkBatchOp extends BaseSinkBatchOp <DBSinkBatchOp> {

	private BaseDB db;
	private String tableName;
	private TableSchema schema = null;

	public DBSinkBatchOp(BaseDB db, String tableName) {
		this(db, tableName, new Params());
	}

	public DBSinkBatchOp(BaseDB db, String tableName, Params parameter) {
		this(db,
			new Params().merge(parameter)
				.set(AnnotationUtils.annotationAlias(db.getClass()), tableName)
		);
	}

	public DBSinkBatchOp(BaseDB db, Params parameter) {
		super(AnnotationUtils.annotationName(db.getClass()), db.getParams().merge(parameter));

		this.db = db;
		this.tableName = parameter.getString(AnnotationUtils.annotationAlias(db.getClass()));
	}

	@Override
	public TableSchema getSchema() {
		return this.schema;
	}

	@Override
	public DBSinkBatchOp linkFrom(BatchOperator in) {
		//Get table schema
		this.schema = in.getSchema();

		//Sink to DB
		db.sinkBatch(this.tableName, in.getTable(), this.params);

		return this;
	}
}
