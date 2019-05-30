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

package org.apache.flink.ml.io.table;

import org.apache.flink.ml.batchoperator.BatchOperator;
import org.apache.flink.ml.batchoperator.source.DBSourceBatchOp;
import org.apache.flink.ml.io.BaseDB;
import org.apache.flink.table.api.TableSchema;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Base class of DataBase Table.
 */
public abstract class BaseDbTable {

	public List <String> listPartitionString() {
		return new ArrayList <>();
	}

	public String getComment() {
		return "";
	}

	public String getOwner() {
		return "";
	}

	public Date getCreatedTime() {
		return null;
	}

	public Date getLastDataModifiedTime() {
		return null;
	}

	public long getLife() {
		return -1;
	}

	public abstract String getTableName();

	public String[] getColComments() {
		String[] comments = new String[this.getColNames().length];
		for (int i = 0; i < comments.length; i++) {
			comments[i] = "";
		}
		return comments;
	}

	public abstract TableSchema getSchema();

	public abstract String[] getColNames();

	public abstract Class[] getColTypes();

	public int getColNum() {
		return getColNames().length;
	}

	public long getRowNum() throws Exception {
		DBSourceBatchOp op = new DBSourceBatchOp(getDB(), this.getTableName());
		return op.count();
	}

	public abstract BaseDB getDB();

	public BatchOperator getBatchOperator() throws Exception {
		return new DBSourceBatchOp(this.getDB(), this.getTableName());
	}
}
