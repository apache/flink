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

package org.apache.flink.api.java.operators;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Preconditions;

/**
 * Base class of all operators in the Java API.
 *
 * @param <OUT> The type of the data set produced by this operator.
 * @param <O> The type of the operator, so that we can return it.
 */
@Public
public abstract class Operator<OUT, O extends Operator<OUT, O>> extends DataSet<OUT> {

	protected String name;

	protected int parallelism = ExecutionConfig.PARALLELISM_DEFAULT;

	protected ResourceSpec minResources = ResourceSpec.DEFAULT;

	protected ResourceSpec preferredResources = ResourceSpec.DEFAULT;

	protected Operator(ExecutionEnvironment context, TypeInformation<OUT> resultType) {
		super(context, resultType);
	}

	/**
	 * Returns the type of the result of this operator.
	 *
	 * @return The result type of the operator.
	 */
	public TypeInformation<OUT> getResultType() {
		return getType();
	}

	/**
	 * Returns the name of the operator. If no name has been set, it returns the name of the
	 * operation, or the name of the class implementing the function of this operator.
	 *
	 * @return The name of the operator.
	 */
	public String getName() {
		return name;
	}

	/**
	 * Returns the parallelism of this operator.
	 *
	 * @return The parallelism of this operator.
	 */
	public int getParallelism() {
		return this.parallelism;
	}

	/**
	 * Returns the minimum resource of this operator. If no minimum resource has been set,
	 * it returns the default empty resource.
	 *
	 * @return The minimum resource of this operator.
	 */
	public ResourceSpec getMinResources() {
		return this.minResources;
	}

	/**
	 * Returns the preferred resource of this operator. If no preferred resource has been set,
	 * it returns the default empty resource.
	 *
	 * @return The preferred resource of this operator.
	 */
	public ResourceSpec getPreferredResources() {
		return this.preferredResources;
	}

	/**
	 * Sets the name of this operator. This overrides the default name, which is either
	 * a generated description of the operation (such as for example "Aggregate(1:SUM, 2:MIN)")
	 * or the name the user-defined function or input/output format executed by the operator.
	 *
	 * @param newName The name for this operator.
	 * @return The operator with a new name.
	 */
	public O name(String newName) {
		this.name = newName;
		@SuppressWarnings("unchecked")
		O returnType = (O) this;
		return returnType;
	}

	/**
	 * Sets the parallelism for this operator.
	 * The parallelism must be 1 or more.
	 *
	 * @param parallelism The parallelism for this operator. A value equal to {@link ExecutionConfig#PARALLELISM_DEFAULT}
	 *        will use the system default.
	 * @return The operator with set parallelism.
	 */
	public O setParallelism(int parallelism) {
		Preconditions.checkArgument(parallelism > 0 || parallelism == ExecutionConfig.PARALLELISM_DEFAULT,
				"The parallelism must be at least one, or ExecutionConfig.PARALLELISM_DEFAULT (use system default).");

		this.parallelism = parallelism;

		@SuppressWarnings("unchecked")
		O returnType = (O) this;
		return returnType;
	}

	//	---------------------------------------------------------------------------
	//	 Fine-grained resource profiles are an incomplete work-in-progress feature
	//	 The setters are hence private at this point.
	//	---------------------------------------------------------------------------

	/**
	 * Sets the minimum and preferred resources for this operator. This overrides the default resources.
	 * The lower and upper resource limits will be considered in dynamic resource resize feature for future plan.
	 *
	 * @param minResources The minimum resources for this operator.
	 * @param preferredResources The preferred resources for this operator.
	 * @return The operator with set minimum and preferred resources.
	 */
	private O setResources(ResourceSpec minResources, ResourceSpec preferredResources) {
		Preconditions.checkNotNull(minResources, "The min resources must be not null.");
		Preconditions.checkNotNull(preferredResources, "The preferred resources must be not null.");

		Preconditions.checkArgument(minResources.isValid() && preferredResources.isValid() && minResources.lessThanOrEqual(preferredResources),
				"The values in resources must be not less than 0 and the preferred resources must be greater than the min resources.");

		this.minResources = minResources;
		this.preferredResources = preferredResources;

		@SuppressWarnings("unchecked")
		O returnType = (O) this;
		return returnType;
	}

	/**
	 * Sets the resources for this operator. This overrides the default minimum and preferred resources.
	 *
	 * @param resources The resources for this operator.
	 * @return The operator with set minimum and preferred resources.
	 */
	private O setResources(ResourceSpec resources) {
		Preconditions.checkNotNull(resources, "The resources must be not null.");
		Preconditions.checkArgument(resources.isValid(), "The values in resources must be not less than 0.");

		this.minResources = resources;
		this.preferredResources = resources;

		@SuppressWarnings("unchecked")
		O returnType = (O) this;
		return returnType;
	}
}
