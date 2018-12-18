/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.transformations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.MissingTypeInfo;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Collection;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@code StreamTransformation} represents the operation that creates a
 * {@link org.apache.flink.streaming.api.datastream.DataStream}. Every
 * {@link org.apache.flink.streaming.api.datastream.DataStream} has an underlying
 * {@code StreamTransformation} that is the origin of said DataStream.
 *
 * <p>API operations such as {@link org.apache.flink.streaming.api.datastream.DataStream#map} create
 * a tree of {@code StreamTransformation}s underneath. When the stream program is to be executed
 * this graph is translated to a {@link StreamGraph} using
 * {@link org.apache.flink.streaming.api.graph.StreamGraphGenerator}.
 *
 * <p>A {@code StreamTransformation} does not necessarily correspond to a physical operation
 * at runtime. Some operations are only logical concepts. Examples of this are union,
 * split/select data stream, partitioning.
 *
 * <p>The following graph of {@code StreamTransformations}:
 * <pre>{@code
 *   Source              Source
 *      +                   +
 *      |                   |
 *      v                   v
 *  Rebalance          HashPartition
 *      +                   +
 *      |                   |
 *      |                   |
 *      +------>Union<------+
 *                +
 *                |
 *                v
 *              Split
 *                +
 *                |
 *                v
 *              Select
 *                +
 *                v
 *               Map
 *                +
 *                |
 *                v
 *              Sink
 * }</pre>
 *
 * <p>Would result in this graph of operations at runtime:
 * <pre>{@code
 *  Source              Source
 *    +                   +
 *    |                   |
 *    |                   |
 *    +------->Map<-------+
 *              +
 *              |
 *              v
 *             Sink
 * }</pre>
 *
 * <p>The information about partitioning, union, split/select end up being encoded in the edges
 * that connect the sources to the map operation.
 *
 * @param <T> The type of the elements that result from this {@code StreamTransformation}
 */
@Internal
public abstract class StreamTransformation<T> {

	// This is used to assign a unique ID to every StreamTransformation
	protected static Integer idCounter = 0;

	public static int getNewNodeId() {
		idCounter++;
		return idCounter;
	}


	protected final int id;

	protected String name;

	protected TypeInformation<T> outputType;
	// This is used to handle MissingTypeInfo. As long as the outputType has not been queried
	// it can still be changed using setOutputType(). Afterwards an exception is thrown when
	// trying to change the output type.
	protected boolean typeUsed;

	private int parallelism;

	/**
	 * The maximum parallelism for this stream transformation. It defines the upper limit for
	 * dynamic scaling and the number of key groups used for partitioned state.
	 */
	private int maxParallelism = -1;

	/**
	 *  The minimum resources for this stream transformation. It defines the lower limit for
	 *  dynamic resources resize in future plan.
	 */
	private ResourceSpec minResources = ResourceSpec.DEFAULT;

	/**
	 *  The preferred resources for this stream transformation. It defines the upper limit for
	 *  dynamic resource resize in future plan.
	 */
	private ResourceSpec preferredResources = ResourceSpec.DEFAULT;

	/**
	 * User-specified ID for this transformation. This is used to assign the
	 * same operator ID across job restarts. There is also the automatically
	 * generated {@link #id}, which is assigned from a static counter. That
	 * field is independent from this.
	 */
	private String uid;

	private String userProvidedNodeHash;

	protected long bufferTimeout = -1;

	private String slotSharingGroup;

	@Nullable
	private String coLocationGroupKey;

	/**
	 * Creates a new {@code StreamTransformation} with the given name, output type and parallelism.
	 *
	 * @param name The name of the {@code StreamTransformation}, this will be shown in Visualizations and the Log
	 * @param outputType The output type of this {@code StreamTransformation}
	 * @param parallelism The parallelism of this {@code StreamTransformation}
	 */
	public StreamTransformation(String name, TypeInformation<T> outputType, int parallelism) {
		this.id = getNewNodeId();
		this.name = Preconditions.checkNotNull(name);
		this.outputType = outputType;
		this.parallelism = parallelism;
		this.slotSharingGroup = null;
	}

	/**
	 * Returns the unique ID of this {@code StreamTransformation}.
	 */
	public int getId() {
		return id;
	}

	/**
	 * Changes the name of this {@code StreamTransformation}.
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * Returns the name of this {@code StreamTransformation}.
	 */
	public String getName() {
		return name;
	}

	/**
	 * Returns the parallelism of this {@code StreamTransformation}.
	 */
	public int getParallelism() {
		return parallelism;
	}

	/**
	 * Sets the parallelism of this {@code StreamTransformation}.
	 *
	 * @param parallelism The new parallelism to set on this {@code StreamTransformation}.
	 */
	public void setParallelism(int parallelism) {
		Preconditions.checkArgument(
				parallelism > 0 || parallelism == ExecutionConfig.PARALLELISM_DEFAULT,
				"The parallelism must be at least one, or ExecutionConfig.PARALLELISM_DEFAULT (use system default).");
		this.parallelism = parallelism;
	}

	/**
	 * Gets the maximum parallelism for this stream transformation.
	 *
	 * @return Maximum parallelism of this transformation.
	 */
	public int getMaxParallelism() {
		return maxParallelism;
	}

	/**
	 * Sets the maximum parallelism for this stream transformation.
	 *
	 * @param maxParallelism Maximum parallelism for this stream transformation.
	 */
	public void setMaxParallelism(int maxParallelism) {
		Preconditions.checkArgument(maxParallelism > 0
						&& maxParallelism <= StreamGraphGenerator.UPPER_BOUND_MAX_PARALLELISM,
				"Maximum parallelism must be between 1 and " + StreamGraphGenerator.UPPER_BOUND_MAX_PARALLELISM
						+ ". Found: " + maxParallelism);
		this.maxParallelism = maxParallelism;
	}

	/**
	 * Sets the minimum and preferred resources for this stream transformation.
	 *
	 * @param minResources The minimum resource of this transformation.
	 * @param preferredResources The preferred resource of this transformation.
	 */
	public void setResources(ResourceSpec minResources, ResourceSpec preferredResources) {
		this.minResources = checkNotNull(minResources);
		this.preferredResources = checkNotNull(preferredResources);
	}

	/**
	 * Gets the minimum resource of this stream transformation.
	 *
	 * @return The minimum resource of this transformation.
	 */
	public ResourceSpec getMinResources() {
		return minResources;
	}

	/**
	 * Gets the preferred resource of this stream transformation.
	 *
	 * @return The preferred resource of this transformation.
	 */
	public ResourceSpec getPreferredResources() {
		return preferredResources;
	}

	/**
	 * Sets an user provided hash for this operator. This will be used AS IS the create the
	 * JobVertexID.
	 *
	 * <p>The user provided hash is an alternative to the generated hashes, that is considered when
	 * identifying an operator through the default hash mechanics fails (e.g. because of changes
	 * between Flink versions).
	 *
	 * <p><strong>Important</strong>: this should be used as a workaround or for trouble shooting.
	 * The provided hash needs to be unique per transformation and job. Otherwise, job submission
	 * will fail. Furthermore, you cannot assign user-specified hash to intermediate nodes in an
	 * operator chain and trying so will let your job fail.
	 *
	 * <p>A use case for this is in migration between Flink versions or changing the jobs in a way
	 * that changes the automatically generated hashes. In this case, providing the previous hashes
	 * directly through this method (e.g. obtained from old logs) can help to reestablish a lost
	 * mapping from states to their target operator.
	 *
	 * @param uidHash The user provided hash for this operator. This will become the JobVertexID, which is shown in the
	 *                 logs and web ui.
	 */
	public void setUidHash(String uidHash) {

		Preconditions.checkNotNull(uidHash);
		Preconditions.checkArgument(uidHash.matches("^[0-9A-Fa-f]{32}$"),
				"Node hash must be a 32 character String that describes a hex code. Found: " + uidHash);

		this.userProvidedNodeHash = uidHash;
	}

	/**
	 * Gets the user provided hash.
	 *
	 * @return The user provided hash.
	 */
	public String getUserProvidedNodeHash() {
		return userProvidedNodeHash;
	}

	/**
	 * Sets an ID for this {@link StreamTransformation}. This is will later be hashed to a uidHash which is then used to
	 * create the JobVertexID (that is shown in logs and the web ui).
	 *
	 * <p>The specified ID is used to assign the same operator ID across job
	 * submissions (for example when starting a job from a savepoint).
	 *
	 * <p><strong>Important</strong>: this ID needs to be unique per
	 * transformation and job. Otherwise, job submission will fail.
	 *
	 * @param uid The unique user-specified ID of this transformation.
	 */
	public void setUid(String uid) {
		this.uid = uid;
	}

	/**
	 * Returns the user-specified ID of this transformation.
	 *
	 * @return The unique user-specified ID of this transformation.
	 */
	public String getUid() {
		return uid;
	}

	/**
	 * Returns the slot sharing group of this transformation.
	 *
	 * @see #setSlotSharingGroup(String)
	 */
	public String getSlotSharingGroup() {
		return slotSharingGroup;
	}

	/**
	 * Sets the slot sharing group of this transformation. Parallel instances of operations that
	 * are in the same slot sharing group will be co-located in the same TaskManager slot, if
	 * possible.
	 *
	 * <p>Initially, an operation is in the default slot sharing group. This can be explicitly
	 * set using {@code setSlotSharingGroup("default")}.
	 *
	 * @param slotSharingGroup The slot sharing group name.
	 */
	public void setSlotSharingGroup(String slotSharingGroup) {
		this.slotSharingGroup = slotSharingGroup;
	}

	/**
	 * <b>NOTE:</b> This is an internal undocumented feature for now. It is not
	 * clear whether this will be supported and stable in the long term.
	 *
	 * <p>Sets the key that identifies the co-location group.
	 * Operators with the same co-location key will have their corresponding subtasks
	 * placed into the same slot by the scheduler.
	 *
	 * <p>Setting this to null means there is no co-location constraint.
	 */
	public void setCoLocationGroupKey(@Nullable String coLocationGroupKey) {
		this.coLocationGroupKey = coLocationGroupKey;
	}

	/**
	 * <b>NOTE:</b> This is an internal undocumented feature for now. It is not
	 * clear whether this will be supported and stable in the long term.
	 *
	 * <p>Gets the key that identifies the co-location group.
	 * Operators with the same co-location key will have their corresponding subtasks
	 * placed into the same slot by the scheduler.
	 *
	 * <p>If this is null (which is the default), it means there is no co-location constraint.
	 */
	@Nullable
	public String getCoLocationGroupKey() {
		return coLocationGroupKey;
	}

	/**
	 * Tries to fill in the type information. Type information can be filled in
	 * later when the program uses a type hint. This method checks whether the
	 * type information has ever been accessed before and does not allow
	 * modifications if the type was accessed already. This ensures consistency
	 * by making sure different parts of the operation do not assume different
	 * type information.
	 *
	 * @param outputType The type information to fill in.
	 *
	 * @throws IllegalStateException Thrown, if the type information has been accessed before.
	 */
	public void setOutputType(TypeInformation<T> outputType) {
		if (typeUsed) {
			throw new IllegalStateException(
					"TypeInformation cannot be filled in for the type after it has been used. "
							+ "Please make sure that the type info hints are the first call after"
							+ " the transformation function, "
							+ "before any access to types or semantic properties, etc.");
		}
		this.outputType = outputType;
	}

	/**
	 * Returns the output type of this {@code StreamTransformation} as a {@link TypeInformation}. Once
	 * this is used once the output type cannot be changed anymore using {@link #setOutputType}.
	 *
	 * @return The output type of this {@code StreamTransformation}
	 */
	public TypeInformation<T> getOutputType() {
		if (outputType instanceof MissingTypeInfo) {
			MissingTypeInfo typeInfo = (MissingTypeInfo) this.outputType;
			throw new InvalidTypesException(
					"The return type of function '"
							+ typeInfo.getFunctionName()
							+ "' could not be determined automatically, due to type erasure. "
							+ "You can give type information hints by using the returns(...) "
							+ "method on the result of the transformation call, or by letting "
							+ "your function implement the 'ResultTypeQueryable' "
							+ "interface.", typeInfo.getTypeException());
		}
		typeUsed = true;
		return this.outputType;
	}

	/**
	 * Sets the chaining strategy of this {@code StreamTransformation}.
	 */
	public abstract void setChainingStrategy(ChainingStrategy strategy);

	/**
	 * Set the buffer timeout of this {@code StreamTransformation}. The timeout defines how long data
	 * may linger in a partially full buffer before being sent over the network.
	 *
	 * <p>Lower timeouts lead to lower tail latencies, but may affect throughput.
	 * For Flink 1.5+, timeouts of 1ms are feasible for jobs with high parallelism.
	 *
	 * <p>A value of -1 means that the default buffer timeout should be used. A value
	 * of zero indicates that no buffering should happen, and all records/events should be
	 * immediately sent through the network, without additional buffering.
	 */
	public void setBufferTimeout(long bufferTimeout) {
		checkArgument(bufferTimeout >= -1);
		this.bufferTimeout = bufferTimeout;
	}

	/**
	 * Returns the buffer timeout of this {@code StreamTransformation}.
	 *
	 * @see #setBufferTimeout(long)
	 */
	public long getBufferTimeout() {
		return bufferTimeout;
	}

	/**
	 * Returns all transitive predecessor {@code StreamTransformation}s of this {@code StreamTransformation}. This
	 * is, for example, used when determining whether a feedback edge of an iteration
	 * actually has the iteration head as a predecessor.
	 *
	 * @return The list of transitive predecessors.
	 */
	public abstract Collection<StreamTransformation<?>> getTransitivePredecessors();

	@Override
	public String toString() {
		return getClass().getSimpleName() + "{" +
				"id=" + id +
				", name='" + name + '\'' +
				", outputType=" + outputType +
				", parallelism=" + parallelism +
				'}';
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof StreamTransformation)) {
			return false;
		}

		StreamTransformation<?> that = (StreamTransformation<?>) o;

		if (bufferTimeout != that.bufferTimeout) {
			return false;
		}
		if (id != that.id) {
			return false;
		}
		if (parallelism != that.parallelism) {
			return false;
		}
		if (!name.equals(that.name)) {
			return false;
		}
		return outputType != null ? outputType.equals(that.outputType) : that.outputType == null;
	}

	@Override
	public int hashCode() {
		int result = id;
		result = 31 * result + name.hashCode();
		result = 31 * result + (outputType != null ? outputType.hashCode() : 0);
		result = 31 * result + parallelism;
		result = 31 * result + (int) (bufferTimeout ^ (bufferTimeout >>> 32));
		return result;
	}
}
