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

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.jobmanager.splitassigner.DefaultInputSplitAssigner;
import eu.stratosphere.nephele.template.GenericInputSplit;

public class GeneratorInputSplit extends GenericInputSplit {

	/** First index to read from the values. (inclusive) */
	int start;

	/** Last index to read. (exclusive) */
	int end;

	/** Empty constructor for serialization. */
	public GeneratorInputSplit() {
	}

	private static final String INPUT_SPLIT_CONFIG_KEY_PREFIX = "inputsplit.assigner.";
	
	static {
		final String assignerKey = INPUT_SPLIT_CONFIG_KEY_PREFIX + GeneratorInputSplit.class.getSimpleName();
		Configuration assignerConfig = new Configuration();
		assignerConfig.setClass(assignerKey, DefaultInputSplitAssigner.class);
		GlobalConfiguration.includeConfiguration(assignerConfig);
	}
	
	public GeneratorInputSplit(final int num, final int start, final int end) {
		super(num);
		this.start = start;
		this.end = end;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.template.GenericInputSplit#toString()
	 */
	@Override
	public String toString() {
		return "GeneratorInputSplit[" + this.number + "," + this.start + "," + this.end + "]";
	}

}