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

package org.apache.flink.api.java.aggregation;

import java.io.Serializable;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;

public abstract class AggregationFunction<T, R> implements Serializable {
	private static final long serialVersionUID = 9082279166205627942L;

	private String name;

	public AggregationFunction(String name) {
		this.name = name;
	}

	public static enum ResultTypeBehavior {
		INPUT,
		FIXED
	}

	public abstract ResultTypeBehavior getResultTypeBehavior();
	
	public abstract BasicTypeInfo<R> getResultType();
	
	public abstract void setInputType(BasicTypeInfo<T> inputType);
	
	public abstract int getFieldPosition();
	
	public abstract void initialize();
	
	public abstract void aggregate(T value);
	
	public abstract R getAggregate();

	@Override
	public String toString() {
		return name + "()";
	}

	protected String getName() {
		return name;
	}

}
