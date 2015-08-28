/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions.sink;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.flink.api.common.io.CleanupWhenUnsuccessful;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.runtime.tasks.StreamingRuntimeContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple implementation of the SinkFunction writing tuples in the specified
 * OutputFormat format. Tuples are collected to a list and written to the file
 * periodically. The target path and the overwrite mode are pre-packaged in
 * format.
 * 
 * @param <IN>
 *            Input type
 */
public abstract class FileSinkFunction<IN> extends RichSinkFunction<IN> {
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(FileSinkFunction.class);
	protected ArrayList<IN> tupleList = new ArrayList<IN>();
	protected volatile OutputFormat<IN> format;
	protected volatile boolean cleanupCalled = false;
	protected int indexInSubtaskGroup;
	protected int currentNumberOfSubtasks;

	public FileSinkFunction(OutputFormat<IN> format) {
		this.format = format;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		StreamingRuntimeContext context = (StreamingRuntimeContext) getRuntimeContext();
		format.configure(context.getTaskStubParameters());
		indexInSubtaskGroup = context.getIndexOfThisSubtask();
		currentNumberOfSubtasks = context.getNumberOfParallelSubtasks();
		format.open(indexInSubtaskGroup, currentNumberOfSubtasks);
	}

	@Override
	public void invoke(IN record) throws Exception {
		tupleList.add(record);
		if (updateCondition()) {
			flush();
		}
	}

	@Override
	public void close() throws IOException {
		if (!tupleList.isEmpty()) {
			flush();
		}
		try {
			format.close();
		} catch (Exception ex) {
			if (LOG.isErrorEnabled()) {
				LOG.error("Error while writing element.", ex);
			}
			try {
				if (!cleanupCalled && format instanceof CleanupWhenUnsuccessful) {
					cleanupCalled = true;
					((CleanupWhenUnsuccessful) format).tryCleanupOnError();
				}
			} catch (Throwable t) {
				LOG.error("Cleanup on error failed.", t);
			}
		}
	}

	protected void flush() {
		try {
			for (IN rec : tupleList) {
				format.writeRecord(rec);
			}
		} catch (Exception ex) {
			try {
				if (LOG.isErrorEnabled()) {
					LOG.error("Error while writing element.", ex);
				}
				if (!cleanupCalled && format instanceof CleanupWhenUnsuccessful) {
					cleanupCalled = true;
					((CleanupWhenUnsuccessful) format).tryCleanupOnError();
				}
			} catch (Throwable t) {
				LOG.error("Cleanup on error failed.", t);
			}
			throw new RuntimeException(ex);
		}
		resetParameters();
	}

	/**
	 * Condition for writing the contents of tupleList and clearing it.
	 * 
	 * @return value of the updating condition
	 */
	protected abstract boolean updateCondition();

	/**
	 * Statements to be executed after writing a batch goes here.
	 */
	protected abstract void resetParameters();

}
