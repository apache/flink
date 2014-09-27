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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStream;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStream;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.types.StringValue;

/**
 * A job graph represents an entire Flink runtime job.
 */
public class JobGraph implements IOReadableWritable {

	/** Size of the buffer to be allocated for transferring attached files. */
	private static final int BUFFERSIZE = 8192;
	
	
	// --------------------------------------------------------------------------------------------
	// Members that define the structure / topology of the graph
	// --------------------------------------------------------------------------------------------
	
	/** List of JAR files required to run this job. */
	private final ArrayList<Path> userJars = new ArrayList<Path>();
	
	/** List of task vertices included in this job graph. */
	private final Map<JobVertexID, AbstractJobVertex> taskVertices = new LinkedHashMap<JobVertexID, AbstractJobVertex>();

	/** The job configuration attached to this job. */
	private final Configuration jobConfiguration = new Configuration();
	
	/** ID of this job. */
	private final JobID jobID;

	/** Name of this job. */
	private String jobName;
	
	private boolean allowQueuedScheduling;
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Constructs a new job graph with no name and a random job ID.
	 */
	public JobGraph() {
		this((String) null);
	}

	/**
	 * Constructs a new job graph with the given name and a random job ID.
	 * 
	 * @param jobName The name of the job
	 */
	public JobGraph(String jobName) {
		this(null, jobName);
	}
	
	/**
	 * Constructs a new job graph with the given name and a random job ID.
	 * 
	 * @param jobId The id of the job
	 * @param jobName The name of the job
	 */
	public JobGraph(JobID jobId, String jobName) {
		this.jobID = jobId == null ? new JobID() : jobId;;
		this.jobName = jobName == null ? "(unnamed job)" : jobName;
	}
	
	/**
	 * Constructs a new job graph with no name and a random job ID.
	 * 
	 * @param vertices The vertices to add to the graph.
	 */
	public JobGraph(AbstractJobVertex... vertices) {
		this(null, vertices);
	}

	/**
	 * Constructs a new job graph with the given name and a random job ID.
	 * 
	 * @param jobName The name of the job.
	 * @param vertices The vertices to add to the graph.
	 */
	public JobGraph(String jobName, AbstractJobVertex... vertices) {
		this(null, jobName, vertices);
	}
	
	/**
	 * Constructs a new job graph with the given name and a random job ID.
	 * 
	 * @param jobId The id of the job.
	 * @param jobName The name of the job.
	 * @param vertices The vertices to add to the graph.
	 */
	public JobGraph(JobID jobId, String jobName, AbstractJobVertex... vertices) {
		this(jobId, jobName);
		
		for (AbstractJobVertex vertex : vertices) {
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
	 * Returns the configuration object for this job if it is set.
	 * 
	 * @return the configuration object for this job, or <code>null</code> if it is not set
	 */
	public Configuration getJobConfiguration() {
		return this.jobConfiguration;
	}
	
	public void setAllowQueuedScheduling(boolean allowQueuedScheduling) {
		this.allowQueuedScheduling = allowQueuedScheduling;
	}
	
	public boolean getAllowQueuedScheduling() {
		return allowQueuedScheduling;
	}

	/**
	 * Adds a new task vertex to the job graph if it is not already included.
	 * 
	 * @param vertex
	 *        the new task vertex to be added
	 */
	public void addVertex(AbstractJobVertex vertex) {
		final JobVertexID id = vertex.getID();
		AbstractJobVertex previous = taskVertices.put(id, vertex);
		
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
	public Iterable<AbstractJobVertex> getVertices() {
		return this.taskVertices.values();
	}
	
	/**
	 * Returns an array of all job vertices that are registered with the job graph. The order in which the vertices
	 * appear in the list is not defined.
	 * 
	 * @return an array of all job vertices that are registered with the job graph
	 */
	public AbstractJobVertex[] getVerticesAsArray() {
		return this.taskVertices.values().toArray(new AbstractJobVertex[this.taskVertices.size()]);
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
	 * Searches for a vertex with a matching ID and returns it.
	 * 
	 * @param id
	 *        the ID of the vertex to search for
	 * @return the vertex with the matching ID or <code>null</code> if no vertex with such ID could be found
	 */
	public AbstractJobVertex findVertexByID(JobVertexID id) {
		return this.taskVertices.get(id);
	}
	
	// --------------------------------------------------------------------------------------------

	public List<AbstractJobVertex> getVerticesSortedTopologicallyFromSources() throws InvalidProgramException {
		// early out on empty lists
		if (this.taskVertices.isEmpty()) {
			return Collections.emptyList();
		}
		
		ArrayList<AbstractJobVertex> sorted = new ArrayList<AbstractJobVertex>(this.taskVertices.size());
		LinkedHashSet<AbstractJobVertex> remaining = new LinkedHashSet<AbstractJobVertex>(this.taskVertices.values());
		
		// start by finding the vertices with no input edges
		// and the ones with disconnected inputs (that refer to some standalone data set)
		{
			Iterator<AbstractJobVertex> iter = remaining.iterator();
			while (iter.hasNext()) {
				AbstractJobVertex vertex = iter.next();
				
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
			
			AbstractJobVertex current = sorted.get(startNodePos++);
			addNodesThatHaveNoNewPredecessors(current, sorted, remaining);
		}
		
		return sorted;
	}
	
	private void addNodesThatHaveNoNewPredecessors(AbstractJobVertex start, ArrayList<AbstractJobVertex> target, LinkedHashSet<AbstractJobVertex> remaining) {
		
		// forward traverse over all produced data sets and all their consumers
		for (IntermediateDataSet dataSet : start.getProducedDataSets()) {
			for (JobEdge edge : dataSet.getConsumers()) {
				
				// a vertex can be added, if it has no predecessors that are still in the 'remaining' set
				AbstractJobVertex v = edge.getTarget();
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
	//  Serialization / Deserialization
	// --------------------------------------------------------------------------------------------
	
	@Override
	public void read(DataInputView in) throws IOException {
		// write the simple fields
		this.jobID.read(in);
		this.jobName = StringValue.readString(in);
		this.jobConfiguration.read(in);
		this.allowQueuedScheduling = in.readBoolean();
		
		final int numVertices = in.readInt();
		
		@SuppressWarnings("resource")
		ObjectInputStream ois = new ObjectInputStream(new DataInputViewStream(in));
		for (int i = 0; i < numVertices; i++) {
			try {
				AbstractJobVertex vertex = (AbstractJobVertex) ois.readObject();
				taskVertices.put(vertex.getID(), vertex);
			}
			catch (ClassNotFoundException e) {
				throw new IOException(e);
			}
		}
		ois.close();

		// Read required jar files
		readRequiredJarFiles(in);
	}


	@Override
	public void write(DataOutputView out) throws IOException {
		
		// write the simple fields
		this.jobID.write(out);
		StringValue.writeString(this.jobName, out);
		this.jobConfiguration.write(out);
		out.writeBoolean(allowQueuedScheduling);
		
		// write the task vertices using java serialization (to resolve references in the object graph)
		out.writeInt(taskVertices.size());
		
		ObjectOutputStream oos = new ObjectOutputStream(new DataOutputViewStream(out));
		for (AbstractJobVertex vertex : this.taskVertices.values()) {
			oos.writeObject(vertex);
		}
		oos.close();
		
		// Write out all required jar files
		writeRequiredJarFiles(out);
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
	 * Returns a (possibly empty) array of paths to JAR files which are required to run the job on a task manager.
	 * 
	 * @return a (possibly empty) array of paths to JAR files which are required to run the job on a task manager
	 */
	public Path[] getJars() {
		return userJars.toArray(new Path[userJars.size()]);
	}
	
	/**
	 * Reads required JAR files from an input stream and adds them to the
	 * library cache manager.
	 * 
	 * @param in
	 *        the data stream to read the JAR files from
	 * @throws IOException
	 *         thrown if an error occurs while reading the stream
	 */
	private void readRequiredJarFiles(final DataInputView in) throws IOException {

		// Do jar files follow;
		final int numJars = in.readInt();

		if (numJars > 0) {

			for (int i = 0; i < numJars; i++) {

				final Path p = new Path();
				p.read(in);
				this.userJars.add(p);

				// Read the size of the jar file
				final long sizeOfJar = in.readLong();

				// Add the jar to the library manager
				LibraryCacheManager.addLibrary(this.jobID, p, sizeOfJar, in);
			}

		}

		// Register this job with the library cache manager
		LibraryCacheManager.register(this.jobID, this.userJars.toArray(new Path[0]));
	}
	
	/**
	 * Writes the JAR files of all vertices in array <code>jobVertices</code> to the specified output stream.
	 * 
	 * @param out
	 *        the output stream to write the JAR files to
	 * @throws IOException
	 *         thrown if an error occurs while writing to the stream
	 */
	private void writeRequiredJarFiles(DataOutputView out) throws IOException {

		// Now check if all the collected jar files really exist
		final FileSystem fs = FileSystem.getLocalFileSystem();

		for (int i = 0; i < this.userJars.size(); i++) {
			if (!fs.exists(this.userJars.get(i))) {
				throw new IOException("Cannot find jar file " + this.userJars.get(i));
			}
		}

		// How many jar files follow?
		out.writeInt(this.userJars.size());

		for (int i = 0; i < this.userJars.size(); i++) {

			final Path jar = this.userJars.get(i);

			// Write out the actual path
			jar.write(out);

			// Write out the length of the file
			final FileStatus file = fs.getFileStatus(jar);
			out.writeLong(file.getLen());

			// Now write the jar file
			final FSDataInputStream inStream = fs.open(this.userJars.get(i));
			final byte[] buf = new byte[BUFFERSIZE];
			int read = inStream.read(buf, 0, buf.length);
			while (read > 0) {
				out.write(buf, 0, read);
				read = inStream.read(buf, 0, buf.length);
			}
		}
	}
}
