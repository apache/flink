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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.common.utils.RowTypeDataStream;
import org.apache.flink.ml.streamoperator.StreamOperator;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

/**
 * Transform the Table to SourceStreamOp.
 */
public final class TableSourceStreamOp extends StreamOperator <TableSourceStreamOp> {

	public TableSourceStreamOp(DataStream <Row> rows, String[] colNames, TypeInformation <?>[] colTypes) {
		this(RowTypeDataStream.toTable(rows, colNames, colTypes));
	}

	public TableSourceStreamOp(Table table) {
		super(null);
		if (null == table) {
			throw new RuntimeException();
		}
		this.table = table;
	}

	@Override
	public TableSourceStreamOp linkFrom(StreamOperator in) {
		throw new UnsupportedOperationException(
			"Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

}
