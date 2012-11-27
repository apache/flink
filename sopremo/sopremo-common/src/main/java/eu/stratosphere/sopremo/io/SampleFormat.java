/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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
package eu.stratosphere.sopremo.io;

import java.io.IOException;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.fs.FileInputSplit;
import eu.stratosphere.pact.common.io.FileInputFormat;
import eu.stratosphere.pact.common.io.statistics.BaseStatistics;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.sopremo.pact.JsonInputFormat;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.util.reflect.ReflectUtil;

/**
 * @author Arvid Heise
 */
public class SampleFormat extends FileInputFormat {

	public static final String SAMPLE_SIZE = "sopremo.sample-format.size";

	public static final String SAMPLE_FORMAT = "sopremo.sample-format.format";

	public static final long DEFAULT_SAMPLE_SIZE = 10;

	private FileInputFormat originalFormat;

	private long currentSample = 0, sampleSize = DEFAULT_SAMPLE_SIZE;

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.pact.common.io.FileInputFormat#configure(eu.stratosphere.nephele.configuration.Configuration)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void configure(Configuration parameters) {
		super.configure(parameters);

		this.sampleSize = SopremoUtil.deserialize(parameters, SAMPLE_SIZE, Long.class, DEFAULT_SAMPLE_SIZE);
		this.originalFormat = ReflectUtil.newInstance((Class<FileInputFormat>)
			SopremoUtil.deserialize(parameters, SAMPLE_FORMAT, Class.class, JsonInputFormat.class));
		this.currentSample = 0;
		this.originalFormat.configure(parameters);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.common.io.FileInputFormat#open(eu.stratosphere.nephele.fs.FileInputSplit)
	 */
	@Override
	public void open(FileInputSplit split) throws IOException {
		super.open(split);
		this.originalFormat.open(split);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.common.generic.io.InputFormat#getStatistics(eu.stratosphere.pact.common.io.statistics.
	 * BaseStatistics)
	 */
	@Override
	public BaseStatistics getStatistics(BaseStatistics cachedStatistics) {
		final BaseStatistics statistics = this.originalFormat.getStatistics(cachedStatistics);

		FileBaseStatistics stats = null;

		if (cachedStatistics != null && statistics instanceof FileBaseStatistics)
			stats = (FileBaseStatistics) statistics;
		else
			stats = new FileBaseStatistics(-1, statistics.getTotalInputSize(), statistics.getAverageRecordWidth());

		long sampleSize = Math.min(this.sampleSize, stats.getNumberOfRecords());
		stats.setTotalInputSize((long) Math.ceil(sampleSize * stats.getAverageRecordWidth()));

		return stats;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.common.generic.io.InputFormat#reachedEnd()
	 */
	@Override
	public boolean reachedEnd() throws IOException {
		return this.currentSample >= this.sampleSize || this.originalFormat.reachedEnd();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.common.generic.io.InputFormat#nextRecord(java.lang.Object)
	 */
	@Override
	public boolean nextRecord(PactRecord record) throws IOException {
		this.currentSample++;
		return this.originalFormat.nextRecord(record);
	}

}
