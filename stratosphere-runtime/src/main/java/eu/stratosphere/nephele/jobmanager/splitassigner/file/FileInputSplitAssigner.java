/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.nephele.jobmanager.splitassigner.file;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import eu.stratosphere.nephele.instance.Instance;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.core.fs.FileInputSplit;
import eu.stratosphere.core.io.InputSplit;
import eu.stratosphere.nephele.executiongraph.ExecutionGroupVertex;
import eu.stratosphere.nephele.executiongraph.ExecutionVertex;
import eu.stratosphere.nephele.jobmanager.splitassigner.InputSplitAssigner;
import eu.stratosphere.nephele.template.AbstractInputTask;
import eu.stratosphere.nephele.template.AbstractInvokable;

/**
 * The file input split assigner is a specific implementation of the {@link InputSplitAssigner} interface for
 * {@link FileInputSplit} objects. The file input split assigner offers to take the storage location of the individual
 * file input splits into account. It attempts to always assign the splits to vertices in a way that the data locality
 * is preserved as well as possible.
 * <p>
 * This class is thread-safe.
 * 
 */
public final class FileInputSplitAssigner implements InputSplitAssigner {

	/**
	 * The logging object which is used to report information and errors.
	 */
	private static final Log LOG = LogFactory.getLog(FileInputSplitAssigner.class);

	private final ConcurrentMap<ExecutionGroupVertex, FileInputSplitList> vertexMap = new ConcurrentHashMap<ExecutionGroupVertex, FileInputSplitList>();


	@Override
	public void registerGroupVertex(final ExecutionGroupVertex groupVertex) {

		if (!FileInputSplit.class.equals(groupVertex.getInputSplitType())) {
			LOG.error(groupVertex.getName() + " produces input splits of type " + groupVertex.getInputSplitType()
				+ " and cannot be handled by this split assigner");
			return;
		}

		// Ignore vertices that do not produce splits
		final InputSplit[] inputSplits = groupVertex.getInputSplits();
		if (inputSplits == null) {
			return;
		}

		if (inputSplits.length == 0) {
			return;
		}

		final FileInputSplitList splitStore = new FileInputSplitList();
		if (this.vertexMap.putIfAbsent(groupVertex, splitStore) != null) {
			LOG.error(groupVertex.getName()
				+ " appears to be already registered with the file input split assigner, ignoring vertex...");
			return;
		}

		synchronized (splitStore) {

			for (int i = 0; i < inputSplits.length; ++i) {
				// TODO: Improve this
				final InputSplit inputSplit = inputSplits[i];
				if (!(inputSplit instanceof FileInputSplit)) {
					LOG.error("Input split " + i + " of vertex " + groupVertex.getName() + " is of type "
						+ inputSplit.getClass() + ", ignoring split...");
					continue;
				}
				splitStore.addSplit((FileInputSplit) inputSplit);
			}

		}
	}


	@Override
	public void unregisterGroupVertex(final ExecutionGroupVertex groupVertex) {
		
		this.vertexMap.remove(groupVertex);
	}


	@Override
	public InputSplit getNextInputSplit(final ExecutionVertex vertex) {

		final ExecutionGroupVertex groupVertex = vertex.getGroupVertex();
		final FileInputSplitList splitStore = this.vertexMap.get(groupVertex);

		if (splitStore == null) {
			return null;
		}

		final Instance instance = vertex.getAllocatedResource().getInstance();
		if (instance == null) {
			LOG.error("Instance is null, returning random split");
			return null;
		}

		return splitStore.getNextInputSplit(instance);
	}

}
