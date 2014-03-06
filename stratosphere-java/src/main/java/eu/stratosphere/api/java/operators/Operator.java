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
package eu.stratosphere.api.java.operators;


import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.typeutils.TypeInformation;


/**
 * @param <OUT> The type of the data set produced by this operator.
 * @param <O> The type of the operator, so that we can return it.
 */
public abstract class Operator<OUT, O extends Operator<OUT, O>> extends DataSet<OUT> {

	private String name;

	protected Operator(ExecutionEnvironment context, TypeInformation<OUT> resultType) {
		super(context, resultType);
	}
	
	
	public TypeInformation<OUT> getResultType() {
		return getType();
	}

	public String getName() {
		return name;
	}

	public O name(String newName) {
		this.name = newName;
		@SuppressWarnings("unchecked")
		O returnType = (O) this;
		return returnType;
	}
}
