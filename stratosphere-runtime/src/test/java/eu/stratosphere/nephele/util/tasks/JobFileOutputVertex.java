/***********************************************************************************************************************
 *
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
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.util.tasks;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.core.fs.Path;
import eu.stratosphere.nephele.jobgraph.AbstractJobOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobVertexID;


public class JobFileOutputVertex extends AbstractJobOutputVertex {

	public static final String PATH_PROPERTY = "outputPath";
	
	/**
	 * The path pointing to the output file/directory.
	 */
	private Path path;


	public JobFileOutputVertex(String name, JobVertexID id, JobGraph jobGraph) {
		super(name, id, jobGraph);
	}
	
	/**
	 * Creates a new job file output vertex with the specified name.
	 * 
	 * @param name
	 *        the name of the new job file output vertex
	 * @param jobGraph
	 *        the job graph this vertex belongs to
	 */
	public JobFileOutputVertex(String name, JobGraph jobGraph) {
		this(name, null, jobGraph);
	}

	/**
	 * Creates a new job file input vertex.
	 * 
	 * @param jobGraph
	 *        the job graph this vertex belongs to
	 */
	public JobFileOutputVertex(JobGraph jobGraph) {
		this(null, jobGraph);
	}

	/**
	 * Sets the path of the file the job file input vertex's task should write to.
	 * 
	 * @param path
	 *        the path of the file the job file input vertex's task should write to
	 */
	public void setFilePath(Path path) {
		this.path = path;
		getConfiguration().setString(PATH_PROPERTY, path.toString());
	}

	/**
	 * Returns the path of the file the job file output vertex's task should write to.
	 * 
	 * @return the path of the file the job file output vertex's task should write to or <code>null</code> if no path
	 *         has yet been set
	 */
	public Path getFilePath() {
		return this.path;
	}

	@Override
	public void read(final DataInput in) throws IOException {
		super.read(in);

		// Read path of the input file
		boolean isNotNull = in.readBoolean();
		if (isNotNull) {
			this.path = new Path();
			this.path.read(in);
		}
	}

	@Override
	public void write(final DataOutput out) throws IOException {
		super.write(out);

		// Write out the path of the input file
		if (this.path == null) {
			out.writeBoolean(false);
		} else {
			out.writeBoolean(true);
			this.path.write(out);
		}
	}
}