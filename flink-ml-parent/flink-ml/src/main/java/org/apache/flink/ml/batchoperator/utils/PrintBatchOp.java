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

package org.apache.flink.ml.batchoperator.utils;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.ml.batchoperator.BatchOperator;
import org.apache.flink.ml.common.utils.RowTypeDataSet;
import org.apache.flink.ml.common.utils.RowUtil;
import org.apache.flink.types.Row;

import java.io.PrintStream;
import java.util.List;

/**
 * Print operation for batch data.
 */
public class PrintBatchOp extends BatchOperator {

	private static PrintStream batchPrintStream = System.err;

	public PrintBatchOp() {
		super(null);
	}

	public PrintBatchOp(Params params) {
		super(params);
	}

	public static void setBatchPrintStream(PrintStream printStream) {
		batchPrintStream = printStream;
	}

	@Override
	public BatchOperator linkFrom(BatchOperator in) {
		this.table = in.getTable();
		if (null != this.table) {
			try {
				List <Row> rows = RowTypeDataSet.fromTable(this.table).collect();
				batchPrintStream.println(RowUtil.formatTitle(this.getColNames()));
				for (Row row : rows) {
					batchPrintStream.println(RowUtil.formatRows(row));
				}
			} catch (Exception ex) {
				throw new RuntimeException(ex);
			}
		}
		return this;
	}

}
