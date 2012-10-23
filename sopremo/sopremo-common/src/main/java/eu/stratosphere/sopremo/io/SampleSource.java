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

import eu.stratosphere.pact.common.io.FileInputFormat;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;

/**
 * @author Arvid Heise
 */
public class SampleSource extends Source {

	/**
	 *
	 */
	private static final long serialVersionUID = 1571535516977715620L;

	public SampleSource() {
		super();
	}

	public SampleSource(Class<? extends FileInputFormat> inputFormat, String inputPath) {
		super(SampleFormat.class, inputPath);
		getParameters().put(SampleFormat.SAMPLE_FORMAT, inputFormat);
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.io.Source#setInputFormat(java.lang.Class)
	 */
	@Override
	public void setInputFormat(Class<? extends FileInputFormat> inputFormat) {
		getParameters().put(SampleFormat.SAMPLE_FORMAT, inputFormat);
	}

	public SampleSource(EvaluationExpression adhocValue) {
		super(adhocValue);
	}

	public SampleSource(String inputPath) {
		super(SampleFormat.class, inputPath);
	}

	public SampleSource(Source normalSource) {
		super(SampleFormat.class, normalSource.getInputPath());
		getParameters().putAll(normalSource.getParameters());
		getParameters().put(SampleFormat.SAMPLE_FORMAT, normalSource.getInputFormat());		
	}

	public long getSampleSize() {
		final Long sampleSize = (Long) getParameters().get(SampleFormat.SAMPLE_SIZE);
		return sampleSize == null ? SampleFormat.DEFAULT_SAMPLE_SIZE : sampleSize;
	}
	
	public void setSampleSize(long sampleSize) {
		getParameters().put(SampleFormat.SAMPLE_SIZE, sampleSize);
	}
	
}
