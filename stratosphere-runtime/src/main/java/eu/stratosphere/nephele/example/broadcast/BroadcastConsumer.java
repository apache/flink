/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.example.broadcast;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

import eu.stratosphere.nephele.io.MutableRecordReader;
import eu.stratosphere.nephele.template.AbstractOutputTask;

/**
 * This is a sample consumer task for the broadcast test job.
 */
public class BroadcastConsumer extends AbstractOutputTask {

	/**
	 * The key to access the output path configuration entry.
	 */
	public static final String OUTPUT_PATH_KEY = "broadcast.output.path";

	/**
	 * The key to access the output path configuration entry.
	 */
	public static final String TOPOLOGY_TREE_KEY = "broadcast.topology.tree";

	/**
	 * The key to access the instance type configuration entry.
	 */
	public static final String INSTANCE_TYPE_KEY = "broadcast.instance.type";

	/**
	 * The record record through which this task receives incoming records.
	 */
	private MutableRecordReader<BroadcastRecord> input;

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void registerInputOutput() {

		this.input = new MutableRecordReader<BroadcastRecord>(this);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void invoke() throws Exception {

		int i = 0;

		final BroadcastRecord record = new BroadcastRecord();

		// Open file
		BufferedWriter writer = null;

		try {

			writer = new BufferedWriter(new FileWriter(getFilename()));

			while (this.input.next(record)) {

				if ((i++ % BroadcastProducer.TIMESTAMP_INTERVAL) == 0) {
					final long timestamp = record.getTimestamp();
					writer.write((System.currentTimeMillis() - timestamp) + "\n");
				}

			}

		} finally {

			if (writer != null) {
				writer.close();
			}
		}
	}

	/**
	 * Constructs and returns the name of the file which is supposed to store the latency values.
	 * 
	 * @return the name of the file which is supposed to store the latency values
	 */
	private String getFilename() {

		final String outputPath = getTaskConfiguration().getString(OUTPUT_PATH_KEY, "");
		final String instanceType = getTaskConfiguration().getString(INSTANCE_TYPE_KEY, "unknown");
		final String topologyTree = getTaskConfiguration().getString(TOPOLOGY_TREE_KEY, "unknown");
		final int numberOfRecords = getTaskConfiguration().getInteger(BroadcastProducer.NUMBER_OF_RECORDS_KEY, 0);

		return outputPath + File.separator + "latency_" + instanceType + "_" + topologyTree + "_"
			+ getCurrentNumberOfSubtasks() + "_" + numberOfRecords + "_" + getIndexInSubtaskGroup() + "_"
			+ getEnvironment().getJobID() + ".dat";

	}
}
