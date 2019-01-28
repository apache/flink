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

package org.apache.flink.addons.hbase.strategy;

import org.apache.flink.addons.hbase.TableInputSplit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import java.io.IOException;

/**
 * Table input split strategy which can be leveraged by {@link org.apache.flink.addons.hbase.AbstractTableInputFormat}.
 *
 * <p>For table input format or table input format generalized of {@link org.apache.flink.types.Row}, they all
 * perform likewise, first by configuring HBase, then creating input splits for sub-tasks, at the end for each
 * sub-task, a result scanner will be created to perform scanning.
 *
 * <p>This interface abstract the common works that need to be done in order to perform a scanning.
 * By using bridge design pattern, different input format classes can choose the specific strategy to use in order
 * to create input splits and create result scanner.
 */
public interface TableInputSplitStrategy {

	/**
	 * Configure table input split strategy.
	 *
	 * @param table the HTable.
	 * @param scan  the scanner.
	 * @throws IOException
	 */
	void configure(HTable table, Scan scan) throws IOException;

	/**
	 * Create input splits.
	 *
	 * @param hbaseConfiguration HBase configuration.
	 * @param minNumSplits       minimum split number.
	 * @return table input splits.
	 * @throws IOException
	 */
	TableInputSplit[] createInputSplits(Configuration hbaseConfiguration, int minNumSplits)
		throws IOException;

	/**
	 * Create result scanner.
	 *
	 * @param hbaseConfiguration HBase configuration.
	 * @param split              the table input split.
	 * @return result scanner.
	 * @throws IOException
	 */
	ResultScanner createResultScanner(Configuration hbaseConfiguration, TableInputSplit split)
		throws IOException;
}
