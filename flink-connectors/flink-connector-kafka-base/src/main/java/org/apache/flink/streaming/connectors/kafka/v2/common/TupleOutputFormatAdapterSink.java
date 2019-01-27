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

package org.apache.flink.streaming.connectors.kafka.v2.common;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/** OutputFormat adapter sink function. */
public class TupleOutputFormatAdapterSink<RECORD> extends RichSinkFunction<Tuple2<Boolean, RECORD>>
		implements ListCheckpointed<byte[]> {
	private static final transient Logger LOG =
		LoggerFactory.getLogger(TupleOutputFormatAdapterSink.class);
	private static final long RETRY_INTERVAL = 100;
	private OutputFormat<Tuple2<Boolean, RECORD>> outputFormat;
	private long retryTimeout = 30 * 60 * 1000; // half an hour

	public TupleOutputFormatAdapterSink(OutputFormat<Tuple2<Boolean, RECORD>> outputFormat) {
		this.outputFormat = outputFormat;
	}

	@Override
	public void open(Configuration config) throws IOException {
		if (RichOutputFormat.class.isAssignableFrom(outputFormat.getClass())) {
			((RichOutputFormat) outputFormat).setRuntimeContext(getRuntimeContext());
		}
		outputFormat.configure(config);
		outputFormat.open(
			getRuntimeContext().getIndexOfThisSubtask(),
			getRuntimeContext().getNumberOfParallelSubtasks());
		LOG.info(
			"Initialized OutputFormatAdapterSink of {}/{} task.",
			getRuntimeContext().getIndexOfThisSubtask(),
			getRuntimeContext().getNumberOfParallelSubtasks());
	}

	@Override
	public void close() throws IOException {
		LOG.info("Closing OutputFormatAdapterSink.");
		outputFormat.close();
	}

	@Override
	public void invoke(Tuple2<Boolean, RECORD> record) throws Exception {
		outputFormat.writeRecord(record);
	}

	public OutputFormat<Tuple2<Boolean, RECORD>> getOutputFormat() {
		return outputFormat;
	}

	/*
	 * If OutputFormat implements Syncable, will invoke sync() when doing checkpoint
	*/
	@Override
	public List<byte[]> snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
		if (outputFormat instanceof Syncable) {
			long startSyncing = System.currentTimeMillis();
			// Retry until successful
			while (true) {
				try {
					((Syncable) outputFormat).sync();
					break;
				} catch (IOException e) {
					LOG.error("Sync output format failed", e);
					try {
						Thread.sleep(RETRY_INTERVAL);
					} catch (InterruptedException e1) {
//						throw new RuntimeException(e1);
					}
				}

				long retryingTimeCost = System.currentTimeMillis() - startSyncing;
				if (retryingTimeCost > retryTimeout) {
					throw new IOException("Retried time out with time cost "
						+ retryingTimeCost + " and time out config " + retryTimeout);
				}
			}
		}
		return null;
	}

	/*
	 * We don't need restoreState now
	*/
	@Override
	public void restoreState(List<byte[]> list) throws Exception {

	}

	@Override
	public String toString() {
		return getClass().getSimpleName() + ":" + outputFormat.toString();
	}
}
