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

package eu.stratosphere.nephele.jobgraph;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.Vector;

import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.core.fs.FSDataInputStream;
import eu.stratosphere.core.fs.FileStatus;
import eu.stratosphere.core.fs.FileSystem;
import eu.stratosphere.core.fs.Path;
import eu.stratosphere.core.io.IOReadableWritable;
import eu.stratosphere.core.io.StringRecord;
import eu.stratosphere.nephele.execution.librarycache.LibraryCacheManager;
import eu.stratosphere.util.ClassUtils;

/**
 * A job graph represents an entire job in Nephele. A job graph must consists at least of one job vertex
 * and must be acyclic.
 * 
 * @author warneke
 */
public class JobGraph implements IOReadableWritable {

	/**
	 * List of input vertices included in this job graph.
	 */
	private Map<JobVertexID, AbstractJobInputVertex> inputVertices = new HashMap<JobVertexID, AbstractJobInputVertex>();

	/**
	 * List of output vertices included in this job graph.
	 */
	private Map<JobVertexID, AbstractJobOutputVertex> outputVertices = new HashMap<JobVertexID, AbstractJobOutputVertex>();

	/**
	 * List of task vertices included in this job graph.
	 */
	private Map<JobVertexID, JobTaskVertex> taskVertices = new HashMap<JobVertexID, JobTaskVertex>();

	/**
	 * ID of this job.
	 */
	private JobID jobID;

	/**
	 * Name of this job.
	 */
	private String jobName;

	/**
	 * The job configuration attached to this job.
	 */
	private Configuration jobConfiguration = new Configuration();

	/**
	 * The configuration which should be applied to the task managers involved in processing this job.
	 */
	private final Configuration taskManagerConfiguration = new Configuration();

	/**
	 * List of JAR files required to run this job.
	 */
	private final ArrayList<Path> userJars = new ArrayList<Path>();

	/**
	 * Size of the buffer to be allocated for transferring attached files.
	 */
	private static final int BUFFERSIZE = 8192;

	/**
	 * Constructs a new job graph with a random job ID.
	 */
	public JobGraph() {
		this.jobID = new JobID();
	}

	/**
	 * Constructs a new job graph with the given name and a random job ID.
	 * 
	 * @param jobName
	 *        the name for this job graph
	 */
	public JobGraph(final String jobName) {
		this();
		this.jobName = jobName;
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

	/**
	 * Returns the configuration object distributed among the task managers
	 * before they start processing this job.
	 * 
	 * @return the configuration object for the task managers, or <code>null</code> if it is not set
	 */
	public Configuration getTaskmanagerConfiguration() {

		return this.taskManagerConfiguration;
	}

	/**
	 * Adds a new input vertex to the job graph if it is not already included.
	 * 
	 * @param inputVertex
	 *        the new input vertex to be added
	 */
	public void addVertex(final AbstractJobInputVertex inputVertex) {

		if (!inputVertices.containsKey(inputVertex.getID())) {
			inputVertices.put(inputVertex.getID(), inputVertex);
		}
	}

	/**
	 * Adds a new task vertex to the job graph if it is not already included.
	 * 
	 * @param taskVertex
	 *        the new task vertex to be added
	 */
	public void addVertex(final JobTaskVertex taskVertex) {

		if (!taskVertices.containsKey(taskVertex.getID())) {
			taskVertices.put(taskVertex.getID(), taskVertex);
		}
	}

	/**
	 * Adds a new output vertex to the job graph if it is not already included.
	 * 
	 * @param outputVertex
	 *        the new output vertex to be added
	 */
	public void addVertex(final AbstractJobOutputVertex outputVertex) {

		if (!outputVertices.containsKey(outputVertex.getID())) {
			outputVertices.put(outputVertex.getID(), outputVertex);
		}
	}

	/**
	 * Returns the number of input vertices registered with the job graph.
	 * 
	 * @return the number of input vertices registered with the job graph
	 */
	public int getNumberOfInputVertices() {
		return this.inputVertices.size();
	}

	/**
	 * Returns the number of output vertices registered with the job graph.
	 * 
	 * @return the number of output vertices registered with the job graph
	 */
	public int getNumberOfOutputVertices() {
		return this.outputVertices.size();
	}

	/**
	 * Returns the number of task vertices registered with the job graph.
	 * 
	 * @return the number of task vertices registered with the job graph
	 */
	public int getNumberOfTaskVertices() {
		return this.taskVertices.size();
	}

	/**
	 * Returns an iterator to iterate all input vertices registered with the job graph.
	 * 
	 * @return an iterator to iterate all input vertices registered with the job graph
	 */
	public Iterator<AbstractJobInputVertex> getInputVertices() {

		final Collection<AbstractJobInputVertex> coll = this.inputVertices.values();

		return coll.iterator();
	}

	/**
	 * Returns an iterator to iterate all output vertices registered with the job graph.
	 * 
	 * @return an iterator to iterate all output vertices registered with the job graph
	 */
	public Iterator<AbstractJobOutputVertex> getOutputVertices() {

		final Collection<AbstractJobOutputVertex> coll = this.outputVertices.values();

		return coll.iterator();
	}

	/**
	 * Returns an iterator to iterate all task vertices registered with the job graph.
	 * 
	 * @return an iterator to iterate all task vertices registered with the job graph
	 */
	public Iterator<JobTaskVertex> getTaskVertices() {

		final Collection<JobTaskVertex> coll = this.taskVertices.values();

		return coll.iterator();
	}

	/**
	 * Returns the number of all job vertices registered with this job graph.
	 * 
	 * @return the number of all job vertices registered with this job graph
	 */
	public int getNumberOfVertices() {

		return this.inputVertices.size() + this.outputVertices.size() + this.taskVertices.size();
	}

	/**
	 * Returns an array of all job vertices than can be reached when traversing the job graph from the input vertices.
	 * 
	 * @return an array of all job vertices than can be reached when traversing the job graph from the input vertices
	 */
	public AbstractJobVertex[] getAllReachableJobVertices() {

		final Vector<AbstractJobVertex> collector = new Vector<AbstractJobVertex>();
		collectVertices(null, collector);
		return collector.toArray(new AbstractJobVertex[0]);
	}

	/**
	 * Returns an array of all job vertices that are registered with the job graph. The order in which the vertices
	 * appear in the list is not defined.
	 * 
	 * @return an array of all job vertices that are registered with the job graph
	 */
	public AbstractJobVertex[] getAllJobVertices() {

		int i = 0;
		final AbstractJobVertex[] vertices = new AbstractJobVertex[inputVertices.size() + outputVertices.size()
			+ taskVertices.size()];

		final Iterator<AbstractJobInputVertex> iv = getInputVertices();
		while (iv.hasNext()) {
			vertices[i++] = iv.next();
		}

		final Iterator<AbstractJobOutputVertex> ov = getOutputVertices();
		while (ov.hasNext()) {
			vertices[i++] = ov.next();
		}

		final Iterator<JobTaskVertex> tv = getTaskVertices();
		while (tv.hasNext()) {
			vertices[i++] = tv.next();
		}

		return vertices;
	}

	/**
	 * Auxiliary method to collect all vertices which are reachable from the input vertices.
	 * 
	 * @param jv
	 *        the currently considered job vertex
	 * @param collector
	 *        a temporary list to store the vertices that have already been visisted
	 */
	private void collectVertices(final AbstractJobVertex jv, final List<AbstractJobVertex> collector) {

		if (jv == null) {
			final Iterator<AbstractJobInputVertex> iter = getInputVertices();
			while (iter.hasNext()) {
				collectVertices(iter.next(), collector);
			}
		} else {

			if (!collector.contains(jv)) {
				collector.add(jv);
			} else {
				return;
			}

			for (int i = 0; i < jv.getNumberOfForwardConnections(); i++) {
				collectVertices(jv.getForwardConnection(i).getConnectedVertex(), collector);
			}
		}
	}

	/**
	 * Returns the ID of the job.
	 * 
	 * @return the ID of the job
	 */
	public JobID getJobID() {
		return this.jobID;
	}

	/**
	 * Searches for a vertex with a matching ID and returns it.
	 * 
	 * @param id
	 *        the ID of the vertex to search for
	 * @return the vertex with the matching ID or <code>null</code> if no vertex with such ID could be found
	 */
	public AbstractJobVertex findVertexByID(final JobVertexID id) {

		if (this.inputVertices.containsKey(id)) {
			return this.inputVertices.get(id);
		}

		if (this.outputVertices.containsKey(id)) {
			return this.outputVertices.get(id);
		}

		if (this.taskVertices.containsKey(id)) {
			return this.taskVertices.get(id);
		}

		return null;
	}

	/**
	 * Checks if the job vertex with the given ID is registered with the job graph.
	 * 
	 * @param id
	 *        the ID of the vertex to search for
	 * @return <code>true</code> if a vertex with the given ID is registered with the job graph, <code>false</code>
	 *         otherwise.
	 */
	private boolean includedInJobGraph(final JobVertexID id) {

		if (this.inputVertices.containsKey(id)) {
			return true;
		}

		if (this.outputVertices.containsKey(id)) {
			return true;
		}

		if (this.taskVertices.containsKey(id)) {
			return true;
		}

		return false;
	}

	/**
	 * Checks if the job graph is weakly connected.
	 * 
	 * @return <code>true</code> if the job graph is weakly connected, otherwise <code>false</code>
	 */
	public boolean isWeaklyConnected() {

		final AbstractJobVertex[] reachable = getAllReachableJobVertices();
		final AbstractJobVertex[] all = getAllJobVertices();

		// Check if number if reachable vertices matches number of registered vertices
		if (reachable.length != all.length) {
			return false;
		}

		final HashMap<JobVertexID, AbstractJobVertex> tmp = new HashMap<JobVertexID, AbstractJobVertex>();
		for (int i = 0; i < reachable.length; i++) {
			tmp.put(reachable[i].getID(), reachable[i]);
		}

		// Check if all is subset of reachable
		for (int i = 0; i < all.length; i++) {
			if (!tmp.containsKey(all[i].getID())) {
				return false;
			}
		}

		// Check if reachable is a subset of all
		for (int i = 0; i < reachable.length; i++) {
			if (!includedInJobGraph(reachable[i].getID())) {
				return false;
			}
		}

		return true;
	}

	/**
	 * Checks if the job graph is acyclic.
	 * 
	 * @return <code>true</code> if the job graph is acyclic, <code>false</code> otherwise
	 */
	public boolean isAcyclic() {

		// Tarjan's algorithm to detect strongly connected componenent of a graph
		final AbstractJobVertex[] reachable = getAllReachableJobVertices();
		final HashMap<AbstractJobVertex, Integer> indexMap = new HashMap<AbstractJobVertex, Integer>();
		final HashMap<AbstractJobVertex, Integer> lowLinkMap = new HashMap<AbstractJobVertex, Integer>();
		final Stack<AbstractJobVertex> stack = new Stack<AbstractJobVertex>();
		final Integer index = Integer.valueOf(0);

		for (int i = 0; i < reachable.length; i++) {
			if (!indexMap.containsKey(reachable[i])) {
				if (!tarjan(reachable[i], index, indexMap, lowLinkMap, stack)) {
					return false;
				}
			}
		}

		return true;
	}

	/**
	 * Auxiliary method implementing Tarjan's algorithm for strongly-connected components to determine whether the job
	 * graph is acyclic.
	 */
	private boolean tarjan(final AbstractJobVertex jv, Integer index,
			final HashMap<AbstractJobVertex, Integer> indexMap, final HashMap<AbstractJobVertex, Integer> lowLinkMap,
			final Stack<AbstractJobVertex> stack) {

		indexMap.put(jv, Integer.valueOf(index));
		lowLinkMap.put(jv, Integer.valueOf(index));
		index = Integer.valueOf(index.intValue() + 1);
		stack.push(jv);

		for (int i = 0; i < jv.getNumberOfForwardConnections(); i++) {

			final AbstractJobVertex jv2 = jv.getForwardConnection(i).getConnectedVertex();
			if (!indexMap.containsKey(jv2) || stack.contains(jv2)) {
				if (!indexMap.containsKey(jv2)) {
					if (!tarjan(jv2, index, indexMap, lowLinkMap, stack)) {
						return false;
					}
				}
				if (lowLinkMap.get(jv) > lowLinkMap.get(jv2)) {
					lowLinkMap.put(jv, Integer.valueOf(lowLinkMap.get(jv2)));
				}
			}
		}

		if (lowLinkMap.get(jv).equals(indexMap.get(jv))) {

			int count = 0;
			while (stack.size() > 0) {
				final AbstractJobVertex jv2 = stack.pop();
				if (jv == jv2) {
					break;
				}

				count++;
			}

			if (count > 0) {
				return false;
			}
		}

		return true;
	}

	/**
	 * Checks for all registered job vertices if their in-/out-degree is correct.
	 * 
	 * @return <code>null</code> if the in-/out-degree of all vertices is correct or the first job vertex whose
	 *         in-/out-degree is incorrect.
	 */
	public AbstractJobVertex areVertexDegreesCorrect() {

		// Check input vertices
		final Iterator<AbstractJobInputVertex> iter = getInputVertices();
		while (iter.hasNext()) {

			final AbstractJobVertex jv = iter.next();

			if (jv.getNumberOfForwardConnections() < 1 || jv.getNumberOfBackwardConnections() > 0) {
				return jv;
			}
		}

		// Check task vertices
		final Iterator<JobTaskVertex> iter2 = getTaskVertices();
		while (iter2.hasNext()) {

			final AbstractJobVertex jv = iter2.next();

			if (jv.getNumberOfForwardConnections() < 1 || jv.getNumberOfBackwardConnections() < 1) {
				return jv;
			}
		}

		// Check output vertices
		final Iterator<AbstractJobOutputVertex> iter3 = getOutputVertices();
		while (iter3.hasNext()) {

			final AbstractJobVertex jv = iter3.next();

			if (jv.getNumberOfForwardConnections() > 0 || jv.getNumberOfBackwardConnections() < 1) {
				return jv;
			}
		}

		return null;
	}


	@Override
	public void read(final DataInput in) throws IOException {

		// Read job id
		this.jobID.read(in);

		// Read the job name
		this.jobName = StringRecord.readString(in);

		// Read required jar files
		readRequiredJarFiles(in);

		// First read total number of vertices;
		final int numVertices = in.readInt();

		// First, recreate each vertex and add it to reconstructionMap
		for (int i = 0; i < numVertices; i++) {
			final String className = StringRecord.readString(in);
			final JobVertexID id = new JobVertexID();
			id.read(in);
			final String vertexName = StringRecord.readString(in);

			Class<? extends IOReadableWritable> c;
			try {
				c = ClassUtils.getRecordByName(className);
			} catch (ClassNotFoundException cnfe) {
				throw new IOException(cnfe.toString());
			}

			// Find constructor
			Constructor<? extends IOReadableWritable> cst;
			try {
				cst = c.getConstructor(String.class, JobVertexID.class, JobGraph.class);
			} catch (SecurityException e1) {
				throw new IOException(e1.toString());
			} catch (NoSuchMethodException e1) {
				throw new IOException(e1.toString());
			}

			try {
				cst.newInstance(vertexName, id, this);
			} catch (IllegalArgumentException e) {
				throw new IOException(e.toString());
			} catch (InstantiationException e) {
				throw new IOException(e.toString());
			} catch (IllegalAccessException e) {
				throw new IOException(e.toString());
			} catch (InvocationTargetException e) {
				throw new IOException(e.toString());
			}
		}

		final JobVertexID tmpID = new JobVertexID();
		for (int i = 0; i < numVertices; i++) {

			AbstractJobVertex jv;

			tmpID.read(in);
			if (inputVertices.containsKey(tmpID)) {
				jv = inputVertices.get(tmpID);
			} else {
				if (outputVertices.containsKey(tmpID)) {
					jv = outputVertices.get(tmpID);
				} else {
					if (taskVertices.containsKey(tmpID)) {
						jv = taskVertices.get(tmpID);
					} else {
						throw new IOException("Cannot find vertex with ID " + tmpID + " in any vertex map.");
					}
				}
			}

			// Read the vertex data
			jv.read(in);
		}

		// Find the class loader for the job
		final ClassLoader cl = LibraryCacheManager.getClassLoader(this.jobID);
		if (cl == null) {
			throw new IOException("Cannot find class loader for job graph " + this.jobID);
		}

		// Re-instantiate the job configuration object and read the configuration
		this.jobConfiguration = new Configuration(cl);
		this.jobConfiguration.read(in);

		// Read the task manager configuration
		this.taskManagerConfiguration.read(in);
	}


	@Override
	public void write(final DataOutput out) throws IOException {

		// Write job ID
		this.jobID.write(out);

		// Write out job name
		StringRecord.writeString(out, this.jobName);

		final AbstractJobVertex[] allVertices = this.getAllJobVertices();

		// Write out all required jar files
		writeRequiredJarFiles(out, allVertices);

		// Write total number of vertices
		out.writeInt(allVertices.length);

		// First write out class name and id for every vertex
		for (int i = 0; i < allVertices.length; i++) {

			final String className = allVertices[i].getClass().getName();
			StringRecord.writeString(out, className);
			allVertices[i].getID().write(out);
			StringRecord.writeString(out, allVertices[i].getName());
		}

		// Now write out vertices themselves
		for (int i = 0; i < allVertices.length; i++) {
			allVertices[i].getID().write(out);
			allVertices[i].write(out);
		}

		// Write out configuration objects
		this.jobConfiguration.write(out);
		this.taskManagerConfiguration.write(out);
	}

	/**
	 * Writes the JAR files of all vertices in array <code>jobVertices</code> to the specified output stream.
	 * 
	 * @param out
	 *        the output stream to write the JAR files to
	 * @param jobVertices
	 *        array of job vertices whose required JAR file are to be written to the output stream
	 * @throws IOException
	 *         thrown if an error occurs while writing to the stream
	 */
	private void writeRequiredJarFiles(final DataOutput out, final AbstractJobVertex[] jobVertices) throws IOException {

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

	/**
	 * Reads required JAR files from an input stream and adds them to the
	 * library cache manager.
	 * 
	 * @param in
	 *        the data stream to read the JAR files from
	 * @throws IOException
	 *         thrown if an error occurs while reading the stream
	 */
	private void readRequiredJarFiles(final DataInput in) throws IOException {

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
	 * Adds the path of a JAR file required to run the job on a task manager.
	 * 
	 * @param jar
	 *        path of the JAR file required to run the job on a task manager
	 */
	public void addJar(final Path jar) {

		if (jar == null) {
			return;
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
	 * Checks if any vertex of this job graph has an outgoing edge which is set to <code>null</code>. If this is the
	 * case the respective vertex is returned.
	 * 
	 * @return the vertex which has an outgoing edge set to <code>null</code> or <code>null</code> if no such vertex
	 *         exists
	 */
	public AbstractJobVertex findVertexWithNullEdges() {

		final AbstractJobVertex[] allVertices = getAllJobVertices();

		for (int i = 0; i < allVertices.length; i++) {

			for (int j = 0; j < allVertices[i].getNumberOfForwardConnections(); j++) {
				if (allVertices[i].getForwardConnection(j) == null) {
					return allVertices[i];
				}
			}

			for (int j = 0; j < allVertices[i].getNumberOfBackwardConnections(); j++) {
				if (allVertices[i].getBackwardConnection(j) == null) {
					return allVertices[i];
				}
			}
		}

		return null;
	}

	/**
	 * Checks if the instance dependency chain created with the <code>setVertexToShareInstancesWith</code> method is
	 * acyclic.
	 * 
	 * @return <code>true</code> if the dependency chain is acyclic, <code>false</code> otherwise
	 */
	public boolean isInstanceDependencyChainAcyclic() {

		final AbstractJobVertex[] allVertices = this.getAllJobVertices();
		final Set<AbstractJobVertex> alreadyVisited = new HashSet<AbstractJobVertex>();

		for (AbstractJobVertex vertex : allVertices) {

			if (alreadyVisited.contains(vertex)) {
				continue;
			}

			AbstractJobVertex vertexToShareInstancesWith = vertex.getVertexToShareInstancesWith();
			if (vertexToShareInstancesWith != null) {

				final Set<AbstractJobVertex> cycleMap = new HashSet<AbstractJobVertex>();

				while (vertexToShareInstancesWith != null) {

					if (cycleMap.contains(vertexToShareInstancesWith)) {
						return false;
					} else {
						alreadyVisited.add(vertexToShareInstancesWith);
						cycleMap.add(vertexToShareInstancesWith);
						vertexToShareInstancesWith = vertexToShareInstancesWith.getVertexToShareInstancesWith();
					}
				}
			}
		}

		return true;
	}
}
