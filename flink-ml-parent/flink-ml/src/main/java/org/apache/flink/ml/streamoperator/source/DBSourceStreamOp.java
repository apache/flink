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

package org.apache.flink.ml.streamoperator.source;

import org.apache.flink.ml.io.AnnotationUtils;
import org.apache.flink.ml.io.BaseDB;
import org.apache.flink.ml.params.Params;
import org.apache.flink.table.api.Table;

/**
 * Streaming source for DataBase.
 */
public final class DBSourceStreamOp extends BaseSourceStreamOp <DBSourceStreamOp> {
	private BaseDB db;

	public DBSourceStreamOp(BaseDB db, String tableName) throws Exception {
		this(db, tableName, null);
	}

	public DBSourceStreamOp(BaseDB db, String tableName, Params parameter) throws Exception {
		this(db,
			new Params().merge(parameter)
				.set(AnnotationUtils.annotationAlias(db.getClass()), tableName)
		);
	}

	public DBSourceStreamOp(BaseDB db, Params parameter) throws Exception {
		super(AnnotationUtils.annotationName(db.getClass()), db.getParams().merge(parameter));
		this.db = db;
	}

	@Override
	public Table initializeDataSource() {
		String tableName = this.params.getString(AnnotationUtils.annotationAlias(db.getClass()));
		try {
			return db.getStreamTable(tableName, this.params);
		} catch (Exception e) {
			throw new RuntimeException("Fail to get table from db: " + e);
		}
	}
}
