/**
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

import com.google.common.base.Preconditions;

import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.MissingTypeInfo;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.operators.ChainingStrategy;

import java.util.Collection;

/**
 * A {@code StreamTransformation} represents the operation that creates a
 * {@link org.apache.flink.streaming.api.datastream.DataStream}. Every
 * {@link org.apache.flink.streaming.api.datastream.DataStream} has an underlying
 * {@code StreamTransformation} that is the origin of said DataStream.
 *
 * <p>
 * API operations such as {@link org.apache.flink.streaming.api.datastream.DataStream#map} create
 * a tree of {@code StreamTransformation}s underneath. When the stream program is to be executed this
 * graph is translated to a {@link StreamGraph} using
 * {@link org.apache.flink.streaming.api.graph.StreamGraphGenerator}.
 *
 * <p>
 * A {@code StreamTransformation} does not necessarily correspond to a physical operation
 * at runtime. Some operations are only logical concepts. Examples of this are union,
 * split/select data stream, partitioning.
 *
 * <p>
 * The following graph of {@code StreamTransformations}:
 *
 * <pre>
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
 * </pre>
 *
 * Would result in this graph of operations at runtime:
 *
 * <pre>
 *  Source              Source
 *    +                   +
 *    |                   |
 *    |                   |
 *    +------->Map<-------+
 *              +
 *              |
 *              v
 *             Sink
 * </pre>
 *
 * The information about partitioning, union, split/select end up being encoded in the edges
 * that connect the sources to the map operation.
 *
 * @param <T> The type of the elements that result from this {@code StreamTransformation}
 */
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

	protected long bufferTimeout = -1;

	protected StreamGraph.ResourceStrategy resourceStrategy = StreamGraph.ResourceStrategy.DEFAULT;

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
	 * Returns the parallelism of this {@code StreamTransformation}
	 */
	public int getParallelism() {
		return parallelism;
	}

	/**
	 * Sets the parallelism of this {@code StreamTransformation}
	 * @param parallelism The new parallelism to set on this {@code StreamTransformation}
	 */
	public void setParallelism(int parallelism) {
		Preconditions.checkArgument(parallelism > 0, "Parallelism must be bigger than zero.");
		this.parallelism = parallelism;
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
	 * Set the buffer timeout of this {@code StreamTransformation}. The timeout is used when
	 * sending elements over the network. The timeout specifies how long a network buffer
	 * should be kept waiting before sending. A higher timeout means that more elements will
	 * be sent in one buffer, this increases throughput. The latency, however, is negatively
	 * affected by a higher timeout.
	 */
	public void setBufferTimeout(long bufferTimeout) {
		this.bufferTimeout = bufferTimeout;
	}

	/**
	 * Returns the buffer timeout of this {@code StreamTransformation}.
	 *
	 * <p>
	 * {@see #setBufferTimeout}
	 */
	public long getBufferTimeout() {
		return bufferTimeout;
	}

	/**
	 * Sets the {@link org.apache.flink.streaming.api.graph.StreamGraph.ResourceStrategy} of this
	 * {@code StreamTransformation}. The resource strategy is used when scheduling operations on actual
	 * workers when transforming the StreamTopology to an
	 * {@link org.apache.flink.runtime.executiongraph.ExecutionGraph}.
	 */
	public void setResourceStrategy(StreamGraph.ResourceStrategy resourceStrategy) {
		this.resourceStrategy = resourceStrategy;
	}

	/**
	 * Returns the {@code ResourceStrategy} of this {@code StreamTransformation}.
	 *
	 * <p>
	 * {@see #setResourceStrategy}
	 */
	public StreamGraph.ResourceStrategy getResourceStrategy() {
		return resourceStrategy;
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
		if (outputType != null ? !outputType.equals(that.outputType) : that.outputType != null) {
			return false;
		}
		return resourceStrategy == that.resourceStrategy;
	}

	@Override
	public int hashCode() {
		int result = id;
		result = 31 * result + name.hashCode();
		result = 31 * result + (outputType != null ? outputType.hashCode() : 0);
		result = 31 * result + parallelism;
		result = 31 * result + (int) (bufferTimeout ^ (bufferTimeout >>> 32));
		result = 31 * result + resourceStrategy.hashCode();
		return result;
	}
}
