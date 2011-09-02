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

package eu.stratosphere.nephele.jobmanager.splitassigner;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.executiongraph.ExecutionGroupVertex;
import eu.stratosphere.nephele.executiongraph.ExecutionGroupVertexIterator;
import eu.stratosphere.nephele.executiongraph.ExecutionVertex;
import eu.stratosphere.nephele.fs.FileInputSplit;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.jobmanager.scheduler.AbstractScheduler;
import eu.stratosphere.nephele.jobmanager.splitassigner.file.FileInputSplitAssigner;
import eu.stratosphere.nephele.template.AbstractInputTask;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.nephele.template.InputSplit;
import eu.stratosphere.nephele.util.StringUtils;

/**
 * The input split manager is responsible for serving input splits to {@link AbstractInputTask} objects at runtime.
 * Before passed on to the {@link AbstractScheduler}, an {@link ExecutionGraph} is registered with the input split
 * manager and all included input vertices of the graph register their generated input splits with the manager. Each
 * type of input split can be assigned to a specific {@link InputSplitAssigner} which is loaded by the input split
 * manager at runtime.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 */
public final class InputSplitManager {

	/**
	 * The logging object which is used to report information and errors.
	 */
	private static final Log LOG = LogFactory.getLog(InputSplitManager.class);

	/**
	 * The prefix of the configuration key which is used to retrieve the class names of the individual
	 * {@link InputSplitAssigner} classes
	 */
	private static final String INPUT_SPLIT_CONFIG_KEY_PREFIX = "inputsplit.assigner.";

	/**
	 * A cache which stores the mapping of group vertices to assigner objects for fast retrieval during the job
	 * execution.
	 */
	private final Map<ExecutionGroupVertex, InputSplitAssigner> assignerCache = new ConcurrentHashMap<ExecutionGroupVertex, InputSplitAssigner>();

	/**
	 * A map holding an instance of each available {@link InputSplitAssigner}, accessible via the class name of the
	 * corresponding split type.
	 */
	private final Map<Class<? extends InputSplit>, InputSplitAssigner> loadedAssigners = new HashMap<Class<? extends InputSplit>, InputSplitAssigner>();

	/**
	 * The default input split assigner which is always used if a more specific assigner cannot be found.
	 */
	private final InputSplitAssigner defaultAssigner = new DefaultInputSplitAssigner();

	/**
	 * Registers a new job represented by its {@link ExecutionGraph} with the input split manager.
	 * 
	 * @param executionGraph
	 *        the job to be registered
	 */
	public void registerJob(final ExecutionGraph executionGraph) {

		final Iterator<ExecutionGroupVertex> it = new ExecutionGroupVertexIterator(executionGraph, true, -1);
		while (it.hasNext()) {

			final ExecutionGroupVertex groupVertex = it.next();
			final InputSplit[] inputSplits = groupVertex.getInputSplits();

			if (inputSplits == null) {
				continue;
			}

			if (inputSplits.length == 0) {
				continue;
			}

			final AbstractInvokable invokable = groupVertex.getGroupMember(0).getEnvironment().getInvokable();
			if (!(invokable instanceof AbstractInputTask)) {
				LOG.error(groupVertex.getName() + " has " + inputSplits.length
					+ " input splits, but is not of typt AbstractInputTask, ignoring...");
				continue;
			}

			@SuppressWarnings("unchecked")
			final AbstractInputTask<? extends InputSplit> inputTask = (AbstractInputTask<? extends InputSplit>) invokable;
			final Class<? extends InputSplit> splitType = inputTask.getInputSplitType();

			final InputSplitAssigner assigner = getAssignerByType(splitType, true);
			// Add entry to cache for fast retrieval during the job execution
			this.assignerCache.put(groupVertex, assigner);

			assigner.registerGroupVertex(groupVertex);
		}

	}

	/**
	 * Unregisters the given job represented by its {@link ExecutionGraph} with the input split manager.
	 * 
	 * @param executionGraph
	 *        the job to be unregistered
	 */
	public void unregisterJob(final ExecutionGraph executionGraph) {

		final Iterator<ExecutionGroupVertex> it = new ExecutionGroupVertexIterator(executionGraph, true, -1);
		while (it.hasNext()) {

			final ExecutionGroupVertex groupVertex = it.next();
			final InputSplit[] inputSplits = groupVertex.getInputSplits();

			if (inputSplits == null) {
				continue;
			}

			if (inputSplits.length == 0) {
				continue;
			}

			final InputSplitAssigner assigner = this.assignerCache.remove(groupVertex);
			if (assigner == null) {
				LOG.error("Group vertex " + groupVertex.getName()
					+ " is unregistered, but cannot be found in assigner cache");
				continue;
			}

			assigner.unregisterGroupVertex(groupVertex);
		}
	}

	/**
	 * Returns the next input split the input split manager (or the responsible {@link InputSplitAssigner} to be more
	 * precise) has chosen for the given vertex to consume.
	 * 
	 * @param vertex
	 *        the vertex for which the next input split is to be determined
	 * @return the next input split to consume or <code>null</code> if the vertex shall consume no more input splits
	 */
	public InputSplit getNextInputSplit(final ExecutionVertex vertex) {

		final ExecutionGroupVertex groupVertex = vertex.getGroupVertex();
		final InputSplitAssigner inputSplitAssigner = this.assignerCache.get(groupVertex);
		if (inputSplitAssigner == null) {
			final JobID jobID = groupVertex.getExecutionStage().getExecutionGraph().getJobID();
			LOG.error("Cannot find input assigner for group vertex " + groupVertex.getName() + " (job " + jobID + ")");
			return null;
		}

		final InputSplit nextInputSplit = inputSplitAssigner.getNextInputSplit(vertex);
		if (nextInputSplit != null) {
			LOG.debug(vertex + " receives input split " + nextInputSplit.getPartitionNumber());
		}

		return nextInputSplit;
	}

	/**
	 * Returns the {@link InputSplitAssigner} which is defined for the given type of input split.
	 * 
	 * @param inputSplitType
	 *        the type of input split to find the corresponding {@link InputSplitAssigner} for
	 * @param allowLoading
	 *        <code>true</code> to indicate that the input split assigner is allowed to load additional classes if
	 *        necessary, <code>false</code> otherwise
	 * @return the {@link InputSplitAssigner} responsible for the given type of input split
	 */
	private InputSplitAssigner getAssignerByType(final Class<? extends InputSplit> inputSplitType,
			final boolean allowLoading) {

		synchronized (this.loadedAssigners) {

			InputSplitAssigner assigner = this.loadedAssigners.get(inputSplitType);
			if (assigner == null && allowLoading) {
				assigner = loadInputSplitAssigner(inputSplitType);
				if (assigner != null) {
					this.loadedAssigners.put(inputSplitType, assigner);
				}
			}

			if (assigner != null) {
				return assigner;
			}
		}

		LOG.warn("Unable to find specific input split provider for type " + inputSplitType.getName()
			+ ", using default assigner");

		return this.defaultAssigner;
	}

	/**
	 * Attempts to find the responsible type of {@link InputSplitAssigner} for the given type of input split from the
	 * configuration and instantiate an object for it.
	 * 
	 * @param inputSplitType
	 *        the type of input split to load the {@link InputSplitAssigner} for
	 * @return the newly loaded {@link InputSplitAssigner} object or <code>null</code> if no such object could be
	 *         located or loaded
	 */
	private InputSplitAssigner loadInputSplitAssigner(final Class<? extends InputSplit> inputSplitType) {

		final String typeClassName = inputSplitType.getSimpleName();
		final String assignerKey = INPUT_SPLIT_CONFIG_KEY_PREFIX + typeClassName;
		LOG.info("Trying to load input split assigner for type " + typeClassName);

		String assignerClassName = GlobalConfiguration.getString(assignerKey, null);

		// Provide hard-wired default configuration for FileInputSplit objects to make configuration more robust
		if (assignerClassName == null && FileInputSplit.class.getSimpleName().equals(typeClassName)) {
			assignerClassName = FileInputSplitAssigner.class.getName();
		}

		try {

			@SuppressWarnings("unchecked")
			final Class<? extends InputSplitAssigner> assignerClass = (Class<? extends InputSplitAssigner>) Class
				.forName(assignerClassName);

			return assignerClass.newInstance();

		} catch (Exception e) {
			LOG.error(StringUtils.stringifyException(e));
		}

		return null;
	}
}
