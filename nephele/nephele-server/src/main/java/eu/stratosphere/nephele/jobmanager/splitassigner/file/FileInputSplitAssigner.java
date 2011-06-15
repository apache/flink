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

package eu.stratosphere.nephele.jobmanager.splitassigner.file;

import eu.stratosphere.nephele.executiongraph.ExecutionGroupVertex;
import eu.stratosphere.nephele.executiongraph.ExecutionVertex;
import eu.stratosphere.nephele.fs.FileInputSplit;
import eu.stratosphere.nephele.jobmanager.splitassigner.InputSplitAssigner;
import eu.stratosphere.nephele.template.InputSplit;

/**
 * The file input split assigner is a specific implementation of the {@link InputSplitAssigner} interface for
 * {@link FileInputSplit} objects. The file input split assigner offers to take the storage location of the individual
 * file input splits into account. It attempts to always assign the splits to vertices in a way that the data locality
 * is preserved as well as possible.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 */
public class FileInputSplitAssigner implements InputSplitAssigner {

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void registerGroupVertex(ExecutionGroupVertex groupVertex) {
		// TODO Auto-generated method stub

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void unregisterGroupVertex(ExecutionGroupVertex groupVertex) {
		// TODO Auto-generated method stub

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public InputSplit getNextInputSplit(ExecutionVertex vertex) {
		// TODO Auto-generated method stub
		return null;
	}

}
