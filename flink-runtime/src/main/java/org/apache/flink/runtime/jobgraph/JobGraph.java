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

package org.apache.flink.runtime.jobgraph;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.blob.BlobClient;
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.jobgraph.tasks.JobSnapshottingSettings;
import org.apache.flink.util.SerializedValue;
import scala.concurrent.duration.FiniteDuration;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The JobGraph represents a Flink dataflow program, at the low level that the JobManager accepts.
 * All programs from higher level APIs are transformed into JobGraphs.
 *
 * <p>The JobGraph is a graph of vertices and intermediate results that are connected together to
 * form a DAG. Note that iterations (feedback edges) are currently not encoded inside the JobGraph
 * but inside certain special vertices that establish the feedback channel amongst themselves.</p>
 *
 * <p>The JobGraph defines the job-wide configuration settings, while each vertex and intermediate result
 * define the characteristics of the concrete operation and intermediate data.</p>
 */
public class JobGraph implements Serializable {

	private static final long serialVersionUID = 1L;

	// --------------------------------------------------------------------------------------------
	// Members that define the structure / topology of the graph
	// --------------------------------------------------------------------------------------------

	/** List of task vertices included in this job graph. */
	private final Map<JobVertexID, JobVertex> taskVertices = new LinkedHashMap<JobVertexID, JobVertex>();

	/** The job configuration attached to this job. */
	private final Configuration jobConfiguration = new Configuration();

	/** Set of JAR files required to run this job. */
	private final List<Path> userJars = new ArrayList<Path>();

	/** Set of blob keys identifying the JAR files required to run this job. */
	private final List<BlobKey> userJarBlobKeys = new ArrayList<BlobKey>();

	/** ID of this job. May be set if specific job id is desired (e.g. session management) */
	private final JobID jobID;

	/** Name of this job. */
	private final String jobName;

	/** The number of seconds after which the corresponding ExecutionGraph is removed at the
	 * job manager after it has been executed. */
	private long sessionTimeout = 0;

	/** flag to enable queued scheduling */
	private boolean allowQueuedScheduling;

	/** The mode in which the job is scheduled */
	private ScheduleMode scheduleMode = ScheduleMode.FROM_SOURCES;

	/** The settings for asynchronous snapshots */
	private JobSnapshottingSettings snapshotSettings;

	/** List of classpaths required to run this job. */
	private List<URL> classpaths = Collections.emptyList();

	/** Job specific execution config */
	private SerializedValue<ExecutionConfig> serializedExecutionConfig;

	/** Savepoint restore settings. */
	private SavepointRestoreSettings savepointRestoreSettings = SavepointRestoreSettings.none();

	// --------------------------------------------------------------------------------------------

	/**
	 * Constructs a new job graph with the given name, the given {@link ExecutionConfig},
	 * and a random job ID. The ExecutionConfig will be serialized and can't be modified afterwards.
	 *
	 * @param jobName The name of the job.
	 */
	public JobGraph(String jobName) {
		this(null, jobName);
	}

	/**
	 * Constructs a new job graph with the given job ID (or a random ID, if {@code null} is passed),
	 * the given name and the given execution configuration (see {@link ExecutionConfig}).
	 * The ExecutionConfig will be serialized and can't be modified afterwards.
	 *
	 * @param jobId The id of the job. A random ID is generated, if {@code null} is passed.
	 * @param jobName The name of the job.
	 */
	public JobGraph(JobID jobId, String jobName) {
		this.jobID = jobId == null ? new JobID() : jobId;
		this.jobName = jobName == null ? "(unnamed job)" : jobName;
		setExecutionConfig(new ExecutionConfig());
	}

	/**
	 * Constructs a new job graph with no name, a random job ID, the given {@link ExecutionConfig}, and
	 * the given job vertices. The ExecutionConfig will be serialized and can't be modified afterwards.
	 *
	 * @param vertices The vertices to add to the graph.
	 */
	public JobGraph(JobVertex... vertices) {
		this(null, vertices);
	}

	/**
	 * Constructs a new job graph with the given name, the given {@link ExecutionConfig}, a random job ID,
	 * and the given job vertices. The ExecutionConfig will be serialized and can't be modified afterwards.
	 *
	 * @param jobName The name of the job.
	 * @param vertices The vertices to add to the graph.
	 */
	public JobGraph(String jobName, JobVertex... vertices) {
		this(null, jobName, vertices);
	}

	/**
	 * Constructs a new job graph with the given name, the given {@link ExecutionConfig},
	 * the given jobId or a random one if null supplied, and the given job vertices.
	 * The ExecutionConfig will be serialized and can't be modified afterwards.
	 *
	 * @param jobId The id of the job. A random ID is generated, if {@code null} is passed.
	 * @param jobName The name of the job.
	 * @param vertices The vertices to add to the graph.
	 */
	public JobGraph(JobID jobId, String jobName, JobVertex... vertices) {
		this(jobId, jobName);

		for (JobVertex vertex : vertices) {
			addVertex(vertex);
		}
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Returns the ID of the job.
	 *
	 * @return the ID of the job
	 */
	public JobID getJobID() {
		return this.jobID;
	}

	/**
	 * Returns the name assigned to the job graph.
	 *
	 * @return the name assigned to the job graph
	 */
	public String getName() {
		return this.jobName;
	}

	/**
	 * Returns the configuration object for this job. Job-wide parameters should be set into that
	 * configuration object.
	 *
	 * @return The configuration object for this job.
	 */
	public Configuration getJobConfiguration() {
		return this.jobConfiguration;
	}

	/**
	 * Returns the {@link ExecutionConfig}
	 *
	 * @return ExecutionConfig
	 */
	public SerializedValue<ExecutionConfig> getSerializedExecutionConfig() {
		return serializedExecutionConfig;
	}

	/**
	 * Gets the timeout after which the corresponding ExecutionGraph is removed at the
	 * job manager after it has been executed.
	 * @return a timeout as a long in seconds.
	 */
	public long getSessionTimeout() {
		return sessionTimeout;
	}

	/**
	 * Sets the timeout of the session in seconds. The timeout specifies how long a job will be kept
	 * in the job manager after it finishes.
	 * @param sessionTimeout The timeout in seconds
	 */
	public void setSessionTimeout(long sessionTimeout) {
		this.sessionTimeout = sessionTimeout;
	}

	public void setAllowQueuedScheduling(boolean allowQueuedScheduling) {
		this.allowQueuedScheduling = allowQueuedScheduling;
	}

	public boolean getAllowQueuedScheduling() {
		return allowQueuedScheduling;
	}

	public void setScheduleMode(ScheduleMode scheduleMode) {
		this.scheduleMode = scheduleMode;
	}

	public ScheduleMode getScheduleMode() {
		return scheduleMode;
	}

	/**
	 * Sets the savepoint restore settings.
	 * @param settings The savepoint restore settings.
	 */
	public void setSavepointRestoreSettings(SavepointRestoreSettings settings) {
		this.savepointRestoreSettings = checkNotNull(settings, "Savepoint restore settings");
	}

	/**
	 * Returns the configured savepoint restore setting.
	 * @return The configured savepoint restore settings.
	 */
	public SavepointRestoreSettings getSavepointRestoreSettings() {
		return savepointRestoreSettings;
	}

	/**
	 * Sets a serialized copy of the passed ExecutionConfig. Further modification of the referenced ExecutionConfig
	 * object will not affect this serialized copy.
	 * @param executionConfig The ExecutionConfig to be serialized.
	 */
	public void setExecutionConfig(ExecutionConfig executionConfig) {
		checkNotNull(executionConfig, "ExecutionConfig must not be null.");
		try {
			this.serializedExecutionConfig = new SerializedValue<>(executionConfig);
		} catch (IOException e) {
			throw new RuntimeException("Could not serialize ExecutionConfig.", e);
		}
	}

	/**
	 * Adds a new task vertex to the job graph if it is not already included.
	 *
	 * @param vertex
	 *        the new task vertex to be added
	 */
	public void addVertex(JobVertex vertex) {
		final JobVertexID id = vertex.getID();
		JobVertex previous = taskVertices.put(id, vertex);

		// if we had a prior association, restore and throw an exception
		if (previous != null) {
			taskVertices.put(id, previous);
			throw new IllegalArgumentException("The JobGraph already contains a vertex with that id.");
		}
	}

	/**
	 * Returns an Iterable to iterate all vertices registered with the job graph.
	 *
	 * @return an Iterable to iterate all vertices registered with the job graph
	 */
	public Iterable<JobVertex> getVertices() {
		return this.taskVertices.values();
	}

	/**
	 * Returns an array of all job vertices that are registered with the job graph. The order in which the vertices
	 * appear in the list is not defined.
	 *
	 * @return an array of all job vertices that are registered with the job graph
	 */
	public JobVertex[] getVerticesAsArray() {
		return this.taskVertices.values().toArray(new JobVertex[this.taskVertices.size()]);
	}

	/**
	 * Returns the number of all vertices.
	 *
	 * @return The number of all vertices.
	 */
	public int getNumberOfVertices() {
		return this.taskVertices.size();
	}

	/**
	 * Sets the settings for asynchronous snapshots. A value of {@code null} means that
	 * snapshotting is not enabled.
	 *
	 * @param settings The snapshot settings, or null, to disable snapshotting.
	 */
	public void setSnapshotSettings(JobSnapshottingSettings settings) {
		this.snapshotSettings = settings;
	}

	/**
	 * Gets the settings for asynchronous snapshots. This method returns null, when
	 * snapshotting is not enabled.
	 *
	 * @return The snapshot settings, or null, if snapshotting is not enabled.
	 */
	public JobSnapshottingSettings getSnapshotSettings() {
		return snapshotSettings;
	}

	/**
	 * Searches for a vertex with a matching ID and returns it.
	 *
	 * @param id
	 *        the ID of the vertex to search for
	 * @return the vertex with the matching ID or <code>null</code> if no vertex with such ID could be found
	 */
	public JobVertex findVertexByID(JobVertexID id) {
		return this.taskVertices.get(id);
	}

	/**
	 * Sets the classpaths required to run the job on a task manager.
	 *
	 * @param paths paths of the directories/JAR files required to run the job on a task manager
	 */
	public void setClasspaths(List<URL> paths) {
		classpaths = paths;
	}

	public List<URL> getClasspaths() {
		return classpaths;
	}

	// --------------------------------------------------------------------------------------------

	public List<JobVertex> getVerticesSortedTopologicallyFromSources() throws InvalidProgramException {
		// early out on empty lists
		if (this.taskVertices.isEmpty()) {
			return Collections.emptyList();
		}

		List<JobVertex> sorted = new ArrayList<JobVertex>(this.taskVertices.size());
		Set<JobVertex> remaining = new LinkedHashSet<JobVertex>(this.taskVertices.values());

		// start by finding the vertices with no input edges
		// and the ones with disconnected inputs (that refer to some standalone data set)
		{
			Iterator<JobVertex> iter = remaining.iterator();
			while (iter.hasNext()) {
				JobVertex vertex = iter.next();

				if (vertex.hasNoConnectedInputs()) {
					sorted.add(vertex);
					iter.remove();
				}
			}
		}

		int startNodePos = 0;

		// traverse from the nodes that were added until we found all elements
		while (!remaining.isEmpty()) {

			// first check if we have more candidates to start traversing from. if not, then the
			// graph is cyclic, which is not permitted
			if (startNodePos >= sorted.size()) {
				throw new InvalidProgramException("The job graph is cyclic.");
			}

			JobVertex current = sorted.get(startNodePos++);
			addNodesThatHaveNoNewPredecessors(current, sorted, remaining);
		}

		return sorted;
	}

	private void addNodesThatHaveNoNewPredecessors(JobVertex start, List<JobVertex> target, Set<JobVertex> remaining) {

		// forward traverse over all produced data sets and all their consumers
		for (IntermediateDataSet dataSet : start.getProducedDataSets()) {
			for (JobEdge edge : dataSet.getConsumers()) {

				// a vertex can be added, if it has no predecessors that are still in the 'remaining' set
				JobVertex v = edge.getTarget();
				if (!remaining.contains(v)) {
					continue;
				}

				boolean hasNewPredecessors = false;

				for (JobEdge e : v.getInputs()) {
					// skip the edge through which we came
					if (e == edge) {
						continue;
					}

					IntermediateDataSet source = e.getSource();
					if (remaining.contains(source.getProducer())) {
						hasNewPredecessors = true;
						break;
					}
				}

				if (!hasNewPredecessors) {
					target.add(v);
					remaining.remove(v);
					addNodesThatHaveNoNewPredecessors(v, target, remaining);
				}
			}
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Handling of attached JAR files
	// --------------------------------------------------------------------------------------------

	/**
	 * Adds the path of a JAR file required to run the job on a task manager.
	 *
	 * @param jar
	 *        path of the JAR file required to run the job on a task manager
	 */
	public void addJar(Path jar) {
		if (jar == null) {
			throw new IllegalArgumentException();
		}

		if (!userJars.contains(jar)) {
			userJars.add(jar);
		}
	}

	/**
	 * Adds the BLOB referenced by the key to the JobGraph's dependencies.
	 *
	 * @param key
	 *        path of the JAR file required to run the job on a task manager
	 */
	public void addBlob(BlobKey key) {
		if (key == null) {
			throw new IllegalArgumentException();
		}

		if (!userJarBlobKeys.contains(key)) {
			userJarBlobKeys.add(key);
		}
	}

	/**
	 * Checks whether the JobGraph has user code JAR files attached.
	 *
	 * @return True, if the JobGraph has user code JAR files attached, false otherwise.
	 */
	public boolean hasUsercodeJarFiles() {
		return this.userJars.size() > 0;
	}

	/**
	 * Returns a set of BLOB keys referring to the JAR files required to run this job.
	 *
	 * @return set of BLOB keys referring to the JAR files required to run this job
	 */
	public List<BlobKey> getUserJarBlobKeys() {
		return this.userJarBlobKeys;
	}

	/**
	 * Uploads the previously added user jar file to the job manager through the job manager's BLOB server.
	 *
	 * @param serverAddress
	 *        the network address of the BLOB server
	 * @throws IOException
	 *         thrown if an I/O error occurs during the upload
	 */
	public void uploadRequiredJarFiles(InetSocketAddress serverAddress) throws IOException {
		if (this.userJars.isEmpty()) {
			return;
		}

		BlobClient bc = null;
		try {
			bc = new BlobClient(serverAddress);

			for (final Path jar : this.userJars) {

				final FileSystem fs = jar.getFileSystem();
				FSDataInputStream is = null;
				try {
					is = fs.open(jar);
					final BlobKey key = bc.put(is);
					this.userJarBlobKeys.add(key);
				}
				finally {
					if (is != null) {
						is.close();
					}
				}
			}
		}
		finally {
			if (bc != null) {
				bc.close();
			}
		}
	}

	/**
	 * Gets the maximum parallelism of all operations in this job graph.
	 * @return The maximum parallelism of this job graph
	 */
	public int getMaximumParallelism() {
		int maxParallelism = -1;
		for (JobVertex vertex : taskVertices.values()) {
			maxParallelism = Math.max(vertex.getParallelism(), maxParallelism);
		}
		return maxParallelism;
	}

	/**
	 * Uploads the previously added user JAR files to the job manager through
	 * the job manager's BLOB server. The respective port is retrieved from the
	 * JobManager. This function issues a blocking call.
	 *
	 * @param jobManager JobManager actor gateway
	 * @param askTimeout Ask timeout
	 * @throws IOException Thrown, if the file upload to the JobManager failed.
	 */
	public void uploadUserJars(ActorGateway jobManager, FiniteDuration askTimeout) throws IOException {
		List<BlobKey> blobKeys = BlobClient.uploadJarFiles(jobManager, askTimeout, userJars);

		for (BlobKey blobKey : blobKeys) {
			if (!userJarBlobKeys.contains(blobKey)) {
				userJarBlobKeys.add(blobKey);
			}
		}
	}

	@Override
	public String toString() {
		return "JobGraph(jobId: " + jobID + ")";
	}
}
