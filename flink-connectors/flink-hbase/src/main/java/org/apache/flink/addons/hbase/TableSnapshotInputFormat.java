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

package org.apache.flink.addons.hbase;

import org.apache.flink.addons.hbase.strategy.TableSnapshotInputSplitStrategyImpl;
import org.apache.flink.addons.hbase.util.HBaseConnectorUtil;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.java.tuple.Tuple;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;

/**
 * {@link InputFormat} subclass that wraps the access for HBase table snapshot.
 */
public abstract class TableSnapshotInputFormat<T extends Tuple> extends AbstractTableInputFormat<T>
	implements ResultToTupleMapper<T> {

	private static final long serialVersionUID = 1L;

	/** HBase configuration. */
	private transient org.apache.hadoop.conf.Configuration conf;

	/**
	 * Creates a new table snapshot input format.
	 *
	 * <p>If {@link #getHBaseRootDir()} is specified and not equal to {@link #DEFAULT_HBASE_ROOT_DIR},
	 * then this value will be used as <tt>hbase.rootdir</tt> in HBase configuration.
	 */
	public TableSnapshotInputFormat() {
		if (DEFAULT_HBASE_ROOT_DIR.equals(getHBaseRootDir())) {
			tableInputSplitStrategy = new TableSnapshotInputSplitStrategyImpl(getTableName(), getSnapshotName(),
				getRestoreDirPath());
		} else {
			tableInputSplitStrategy = new TableSnapshotInputSplitStrategyImpl(getTableName(), getSnapshotName(),
				getRestoreDirPath(), getHBaseRootDir());
		}
	}

	/**
	 * Creates a new table snapshot input format.
	 *
	 * @param conf HBase configuration.
	 */
	public TableSnapshotInputFormat(Configuration conf) {
		this.conf = conf;
		tableInputSplitStrategy = new TableSnapshotInputSplitStrategyImpl(getTableName(), getSnapshotName(),
			getRestoreDirPath(), conf.get(HConstants.HBASE_DIR));
	}

	/**
	 * Creates a {@link Scan} object and opens the {@link HTable} connection.
	 * These are opened here because they are needed in the createInputSplits
	 * which is called before the openInputFormat method.
	 * So the connection is opened in {@link #configure(org.apache.flink.configuration.Configuration)} and closed in {@link #closeInputFormat()}.
	 *
	 * @param parameters The configuration that is to be used
	 * @see org.apache.flink.configuration.Configuration
	 */
	@Override
	public void configure(org.apache.flink.configuration.Configuration parameters) {
		table = HBaseConnectorUtil.createTable(getTableName(), conf);
		super.configure(parameters);
	}

	/**
	 * What snapshot is to be read.
	 *
	 * @return snapshot name
	 */
	protected abstract String getSnapshotName();

	/**
	 * What restore path to use, this path will be used to restore snapshot data.
	 *
	 * <p>The temp dir where the snapshot is restored. This should not be the same prefix
	 * with HBase root directory.
	 *
	 * @return restore path
	 */
	protected abstract String getRestoreDirPath();

	/**
	 * What HBase root directory to use, if this value is set, then it will overwrite
	 * value in HBase configuration.
	 *
	 * @return HBase root directory
	 */
	protected String getHBaseRootDir() {
		return DEFAULT_HBASE_ROOT_DIR;
	}

	/** Default HBase root directory. */
	public static final String DEFAULT_HBASE_ROOT_DIR = "hdfs://yourdomain/hbase";

	@Override
	protected T mapResultToOutType(Result r) {
		return mapResultToTuple(r);
	}
}
