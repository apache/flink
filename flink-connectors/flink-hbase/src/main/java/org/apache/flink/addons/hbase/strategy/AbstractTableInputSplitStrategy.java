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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;

import java.io.IOException;
import java.io.Serializable;

/**
 * Abstract class which wraps some common methods.
 */
public abstract class AbstractTableInputSplitStrategy implements TableInputSplitStrategy, Serializable {

	private static final long serialVersionUID = 1L;

	/** The HTable instance. */
	protected transient HTable table = null;

	/** The scanner. */
	protected transient Scan scan = null;

	/** Table name. */
	protected String tableName;

	@Override
	public void configure(HTable table, Scan scan) throws IOException {
		this.table = table;
		this.scan = scan;
	}

	/**
	 * Pre-check before {@link #createInputSplits(Configuration, int)}}.
	 *
	 * @throws IOException
	 */
	protected void preCheck() throws IOException {
		if (table == null) {
			throw new IOException("The HBase table has not been opened! " +
				"This needs to be done in configure().");
		}
		if (scan == null) {
			throw new IOException("Scan has not been initialized! " +
				"This needs to be done in configure().");
		}
	}
}
