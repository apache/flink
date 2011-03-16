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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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
	 * Returns an iterator to a (possible empty) list of file input splits which is expected to be consumed by this
	 * instance of the {@link AbstractFileInputTask}.
	 * 
	 * @return an iterator to a (possible empty) list of file input splits.
	 */
	public Iterator<FileInputSplit> getFileInputSplits() {

		final InputSplit[] inputSplits = getEnvironment().getInputSplits();

		final List<FileInputSplit> fileInputSplits = new ArrayList<FileInputSplit>();

		for (int i = 0; i < inputSplits.length; i++) {
			fileInputSplits.add((FileInputSplit) inputSplits[i]);
		}

		return fileInputSplits.iterator();
	}
}
