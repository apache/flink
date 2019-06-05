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

package org.apache.flink.ml.common;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.ml.batchoperator.BatchOperator;
import org.apache.flink.ml.batchoperator.source.TableSourceBatchOp;
import org.apache.flink.ml.params.BaseWithParam;
import org.apache.flink.ml.streamoperator.StreamOperator;
import org.apache.flink.ml.streamoperator.source.TableSourceStreamOp;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableImpl;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.io.Serializable;

/**
 * Base class of the algorithm operators(batch or Stream).
 */
public abstract class AlgoOperator<T extends AlgoOperator <T>> implements BaseWithParam <T>, Serializable {

	protected Params params;

	protected Table table = null;

	protected Table[] sideTables = null;

	protected AlgoOperator() {
		this(null);
	}

	protected AlgoOperator(Params params) {
		if (null == params) {
			this.params = new Params();
		} else {
			this.params = params.clone();
		}
	}

	public static AlgoOperator sourceFrom(Table table) {
		TableImpl tableImpl = (TableImpl) table;
		if (tableImpl.getTableEnvironment() instanceof StreamTableEnvironment) {
			return new TableSourceStreamOp(table);
		} else {
			return new TableSourceBatchOp(table);
		}
	}

	@Override
	public Params getParams() {
		if (null == this.params) {
			this.params = new Params();
		}
		return this.params;
	}

	public Table getTable() {
		return this.table;
	}

	protected void setTable(Table table) {
		this.table = table;
	}

	public Table[] getSideTables() {
		return this.sideTables;
	}

	public String[] getColNames() {
		return getSchema().getFieldNames();
	}

	public TypeInformation <?>[] getColTypes() {
		return getSchema().getFieldTypes();
	}

	public TableSchema getSchema() {
		return getTable().getSchema();
	}

	public AlgoOperator print() throws Exception {
		if (this instanceof BatchOperator) {
			return ((BatchOperator) this).print();
		} else {
			return ((StreamOperator) this).print();
		}
	}

	public AlgoOperator select(String param) {
		return sourceFrom(this.table.select(param));
	}

	public AlgoOperator select(String[] colNames) {
		StringBuilder sbd = new StringBuilder();
		for (int i = 0; i < colNames.length; i++) {
			if (i > 0) {
				sbd.append(",");
			}
			sbd.append("`").append(colNames[i]).append("`");
		}
		return select(sbd.toString());
	}
}
