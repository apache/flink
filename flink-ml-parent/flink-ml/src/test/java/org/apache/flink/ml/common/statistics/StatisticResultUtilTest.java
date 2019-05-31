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

package org.apache.flink.ml.common.statistics;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.batchoperator.source.MemSourceBatchOp;
import org.apache.flink.ml.common.matrix.Vector;
import org.apache.flink.ml.common.statistics.basicstatistic.BaseVectorSummary;
import org.apache.flink.ml.common.utils.Types;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Test for StatisticResultUtil.
 */
public class StatisticResultUtilTest {

	@Test
	public void testSummaryHelper() throws Exception {
		TableSchema schema = new TableSchema(
			new String[]{"y", "vec"},
			new TypeInformation<?>[]{Types.STRING, Types.STRING}
		);

		List<Row> rows = new ArrayList<>();
		rows.add(Row.of(new Object[]{"0", "0.1,0.2,0.3"}));
		rows.add(Row.of(new Object[]{"0", "1:0.1,3:0.2,10:0.1"}));
		rows.add(Row.of(new Object[]{"0", "0.2,0.3,0.4"}));
		rows.add(Row.of(new Object[]{"0", "0.1,0.2,0.3,0.7"}));

		MemSourceBatchOp source = new MemSourceBatchOp(rows, schema);

		Tuple2<DataSet<Tuple2<Vector, Row>>, DataSet<BaseVectorSummary>> info =
			StatisticsUtil.summaryHelper(source, null, "vec", new String[]{"y"});

		BaseVectorSummary srt = info.f1.collect().get(0);

		Assert.assertEquals(11, srt.vectorSize());
	}

}
