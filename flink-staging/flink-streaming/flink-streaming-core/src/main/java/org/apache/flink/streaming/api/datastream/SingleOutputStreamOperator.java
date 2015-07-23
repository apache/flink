/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.datastream;

import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.typeutils.TypeInfoParser;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph.ResourceStrategy;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperator.ChainingStrategy;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.api.transformations.StreamTransformation;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;

/**
 * The SingleOutputStreamOperator represents a user defined transformation
 * applied on a {@link DataStream} with one predefined output type.
 *
 * @param <T> The type of the elements in this Stream
 * @param <O> Type of the operator.
 */
public class SingleOutputStreamOperator<T, O extends SingleOutputStreamOperator<T, O>> extends DataStream<T> {

	protected SingleOutputStreamOperator(StreamExecutionEnvironment environment, StreamTransformation<T> transformation) {
		super(environment, transformation);
	}

	/**
	 * Gets the name of the current data stream. This name is
	 * used by the visualization and logging during runtime.
	 *
	 * @return Name of the stream.
	 */
	public String getName() {
		return transformation.getName();
	}

	/**
	 * Sets the name of the current data stream. This name is
	 * used by the visualization and logging during runtime.
	 *
	 * @return The named operator.
	 */
	public SingleOutputStreamOperator<T, O> name(String name){
		transformation.setName(name);
		return this;
	}

	/**
	 * Sets the parallelism for this operator. The degree must be 1 or more.
	 * 
	 * @param parallelism
	 *            The parallelism for this operator.
	 * @return The operator with set parallelism.
	 */
	public SingleOutputStreamOperator<T, O> setParallelism(int parallelism) {
		if (parallelism < 1) {
			throw new IllegalArgumentException("The parallelism of an operator must be at least 1.");
		}

		transformation.setParallelism(parallelism);

		return this;
	}

	/**
	 * Sets the maximum time frequency (ms) for the flushing of the output
	 * buffer. By default the output buffers flush only when they are full.
	 * 
	 * @param timeoutMillis
	 *            The maximum time between two output flushes.
	 * @return The operator with buffer timeout set.
	 */
	public SingleOutputStreamOperator<T, O> setBufferTimeout(long timeoutMillis) {
		transformation.setBufferTimeout(timeoutMillis);
		return this;
	}

	@SuppressWarnings("unchecked")
	public SingleOutputStreamOperator<T, O> broadcast() {
		return (SingleOutputStreamOperator<T, O>) super.broadcast();
	}

	@SuppressWarnings("unchecked")
	public SingleOutputStreamOperator<T, O> shuffle() {
		return (SingleOutputStreamOperator<T, O>) super.shuffle();
	}

	@SuppressWarnings("unchecked")
	public SingleOutputStreamOperator<T, O> forward() {
		return (SingleOutputStreamOperator<T, O>) super.forward();
	}

	@SuppressWarnings("unchecked")
	public SingleOutputStreamOperator<T, O> rebalance() {
		return (SingleOutputStreamOperator<T, O>) super.rebalance();
	}

	@SuppressWarnings("unchecked")
	public SingleOutputStreamOperator<T, O> global() {
		return (SingleOutputStreamOperator<T, O>) super.global();
	}

	/**
	 * Sets the {@link ChainingStrategy} for the given operator affecting the
	 * way operators will possibly be co-located on the same thread for
	 * increased performance.
	 * 
	 * @param strategy
	 *            The selected {@link ChainingStrategy}
	 * @return The operator with the modified chaining strategy
	 */
	private SingleOutputStreamOperator<T, O> setChainingStrategy(ChainingStrategy strategy) {
		this.transformation.setChainingStrategy(strategy);
		return this;
	}

	/**
	 * Turns off chaining for this operator so thread co-location will not be
	 * used as an optimization. </p> Chaining can be turned off for the whole
	 * job by {@link StreamExecutionEnvironment#disableOperatorChaining()}
	 * however it is not advised for performance considerations.
	 * 
	 * @return The operator with chaining disabled
	 */
	public SingleOutputStreamOperator<T, O> disableChaining() {
		return setChainingStrategy(AbstractStreamOperator.ChainingStrategy.NEVER);
	}

	/**
	 * Starts a new task chain beginning at this operator. This operator will
	 * not be chained (thread co-located for increased performance) to any
	 * previous tasks even if possible.
	 * 
	 * @return The operator with chaining set.
	 */
	public SingleOutputStreamOperator<T, O> startNewChain() {
		return setChainingStrategy(AbstractStreamOperator.ChainingStrategy.HEAD);
	}

	/**
	 * Adds a type information hint about the return type of this operator. 
	 * 
	 * <p>
	 * Type hints are important in cases where the Java compiler
	 * throws away generic type information necessary for efficient execution.
	 * 
	 * <p>
	 * This method takes a type information string that will be parsed. A type information string can contain the following
	 * types:
	 *
	 * <ul>
	 * <li>Basic types such as <code>Integer</code>, <code>String</code>, etc.
	 * <li>Basic type arrays such as <code>Integer[]</code>,
	 * <code>String[]</code>, etc.
	 * <li>Tuple types such as <code>Tuple1&lt;TYPE0&gt;</code>,
	 * <code>Tuple2&lt;TYPE0, TYPE1&gt;</code>, etc.</li>
	 * <li>Pojo types such as <code>org.my.MyPojo&lt;myFieldName=TYPE0,myFieldName2=TYPE1&gt;</code>, etc.</li>
	 * <li>Generic types such as <code>java.lang.Class</code>, etc.
	 * <li>Custom type arrays such as <code>org.my.CustomClass[]</code>,
	 * <code>org.my.CustomClass$StaticInnerClass[]</code>, etc.
	 * <li>Value types such as <code>DoubleValue</code>,
	 * <code>StringValue</code>, <code>IntegerValue</code>, etc.</li>
	 * <li>Tuple array types such as <code>Tuple2&lt;TYPE0,TYPE1&gt;[], etc.</code></li>
	 * <li>Writable types such as <code>Writable&lt;org.my.CustomWritable&gt;</code></li>
	 * <li>Enum types such as <code>Enum&lt;org.my.CustomEnum&gt;</code></li>
	 * </ul>
	 *
	 * Example:
	 * <code>"Tuple2&lt;String,Tuple2&lt;Integer,org.my.MyJob$Pojo&lt;word=String&gt;&gt;&gt;"</code>
	 *
	 * @param typeInfoString
	 *            type information string to be parsed
	 * @return This operator with a given return type hint.
	 */
	public O returns(String typeInfoString) {
		if (typeInfoString == null) {
			throw new IllegalArgumentException("Type information string must not be null.");
		}
		return returns(TypeInfoParser.<T>parse(typeInfoString));
	}
	
	/**
	 * Adds a type information hint about the return type of this operator. 
	 * 
	 * <p>
	 * Type hints are important in cases where the Java compiler
	 * throws away generic type information necessary for efficient execution.
	 * 
	 * <p>
	 * This method takes an instance of {@link org.apache.flink.api.common.typeinfo.TypeInformation} such as:
	 * 
	 * <ul>
	 * <li>{@link org.apache.flink.api.common.typeinfo.BasicTypeInfo}</li>
	 * <li>{@link org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo}</li>
	 * <li>{@link org.apache.flink.api.java.typeutils.TupleTypeInfo}</li>
	 * <li>{@link org.apache.flink.api.java.typeutils.PojoTypeInfo}</li>
	 * <li>{@link org.apache.flink.api.java.typeutils.WritableTypeInfo}</li>
	 * <li>{@link org.apache.flink.api.java.typeutils.ValueTypeInfo}</li>
	 * <li>etc.</li>
	 * </ul>
	 *
	 * @param typeInfo
	 *            type information as a return type hint
	 * @return This operator with a given return type hint.
	 */
	public O returns(TypeInformation<T> typeInfo) {
		if (typeInfo == null) {
			throw new IllegalArgumentException("Type information must not be null.");
		}
		transformation.setOutputType(typeInfo);
		@SuppressWarnings("unchecked")
		O returnType = (O) this;
		return returnType;
	}
	
	/**
	 * Adds a type information hint about the return type of this operator. 
	 * 
	 * <p>
	 * Type hints are important in cases where the Java compiler
	 * throws away generic type information necessary for efficient execution.
	 * 
	 * <p>
	 * This method takes a class that will be analyzed by Flink's type extraction capabilities.
	 * 
	 * <p>
	 * Examples for classes are:
	 * <ul>
	 * <li>Basic types such as <code>Integer.class</code>, <code>String.class</code>, etc.</li>
	 * <li>POJOs such as <code>MyPojo.class</code></li>
	 * <li>Classes that <b>extend</b> tuples. Classes like <code>Tuple1.class</code>,<code>Tuple2.class</code>, etc. are <b>not</b> sufficient.</li>
	 * <li>Arrays such as <code>String[].class</code>, etc.</li>
	 * </ul>
	 *
	 * @param typeClass
	 *            class as a return type hint
	 * @return This operator with a given return type hint.
	 */
	@SuppressWarnings("unchecked")
	public O returns(Class<T> typeClass) {
		if (typeClass == null) {
			throw new IllegalArgumentException("Type class must not be null.");
		}
		
		try {
			TypeInformation<T> ti = (TypeInformation<T>) TypeExtractor.createTypeInfo(typeClass);
			return returns(ti);
		}
		catch (InvalidTypesException e) {
			throw new InvalidTypesException("The given class is not suited for providing necessary type information.", e);
		}
	}

	@Override
	protected DataStream<T> setConnectionType(StreamPartitioner<T> partitioner) {
		return new SingleOutputStreamOperator<T, O>(this.getExecutionEnvironment(), new PartitionTransformation<T>(this.getTransformation(), partitioner));
	}

	/**
	 * By default all operators in a streaming job share the same resource
	 * group. Each resource group takes as many task manager slots as the
	 * maximum parallelism operator in that group. Task chaining is only
	 * possible within one resource group. By calling this method, this
	 * operators starts a new resource group and all subsequent operators will
	 * be added to this group unless specified otherwise. </p> Please note that
	 * local executions have by default as many available task slots as the
	 * environment parallelism, so in order to start a new resource group the
	 * degree of parallelism for the operators must be decreased from the
	 * default.
	 * 
	 * @return The operator as a part of a new resource group.
	 */
	public SingleOutputStreamOperator<T, O> startNewResourceGroup() {
		transformation.setResourceStrategy(ResourceStrategy.NEWGROUP);
		return this;
	}

	/**
	 * Isolates the operator in its own resource group. This will cause the
	 * operator to grab as many task slots as its degree of parallelism. If
	 * there are no free resources available, the job will fail to start. It
	 * also disables chaining for this operator </p>All subsequent operators are
	 * assigned to the default resource group.
	 * 
	 * @return The operator with isolated resource group.
	 */
	public SingleOutputStreamOperator<T, O> isolateResources() {
		transformation.setResourceStrategy(ResourceStrategy.ISOLATE);
		return this;
	}

}
