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

package eu.stratosphere.nephele.template;

import eu.stratosphere.nephele.fs.FileInputSplit;

/**
 * Specialized subtype of {@link AbstractInputTask} for tasks which are supposed to generate input from
 * a file. In addition to {@link AbstractInputTask} this class includes a method to query file splits
 * which should be read during the task's execution.
 * 
 * @author warneke
 */
public abstract class AbstractFileInputTask extends AbstractInputTask {

	/**
	 * Returns an array of {@FileInputSplits} which have been assigned to this task.
	 * 
	 * @return an array of file input splits which have been assigned to this task
	 */
	public FileInputSplit[] getFileInputSplits() {

		final InputSplit[] inputSplits = getEnvironment().getInputSplits();

		final FileInputSplit[] fileInputSplits = new FileInputSplit[inputSplits.length];

		for (int i = 0; i < fileInputSplits.length; i++) {
			fileInputSplits[i] = (FileInputSplit) inputSplits[i];
		}

		return fileInputSplits;
	}
}
