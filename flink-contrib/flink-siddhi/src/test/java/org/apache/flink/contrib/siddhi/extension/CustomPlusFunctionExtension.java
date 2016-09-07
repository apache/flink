/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.siddhi.extension;

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.exception.ExecutionPlanCreationException;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.function.FunctionExecutor;
import org.wso2.siddhi.query.api.definition.Attribute;

public class CustomPlusFunctionExtension extends FunctionExecutor {

	private Attribute.Type returnType;

	/**
	 * The initialization method for FunctionExecutor, this method will be called before the other methods
	 *
	 * @param attributeExpressionExecutors are the executors of each function parameters
	 * @param executionPlanContext         the context of the execution plan
	 */
	@Override
	protected void init(ExpressionExecutor[] attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {
		for (ExpressionExecutor expressionExecutor : attributeExpressionExecutors) {
			Attribute.Type attributeType = expressionExecutor.getReturnType();
			if (attributeType == Attribute.Type.DOUBLE) {
				returnType = attributeType;

			} else if ((attributeType == Attribute.Type.STRING) || (attributeType == Attribute.Type.BOOL)) {
				throw new ExecutionPlanCreationException("Plus cannot have parameters with types String or Bool");
			} else {
				returnType = Attribute.Type.LONG;
			}
		}
	}

	/**
	 * The main execution method which will be called upon event arrival
	 * when there are more then one function parameter
	 *
	 * @param data the runtime values of function parameters
	 * @return the function result
	 */
	@Override
	protected Object execute(Object[] data) {
		if (returnType == Attribute.Type.DOUBLE) {
			double total = 0;
			for (Object aObj : data) {
				total += Double.parseDouble(String.valueOf(aObj));
			}

			return total;
		} else {
			long total = 0;
			for (Object aObj : data) {
				total += Long.parseLong(String.valueOf(aObj));
			}
			return total;
		}
	}

	/**
	 * The main execution method which will be called upon event arrival
	 * when there are zero or one function parameter
	 *
	 * @param data null if the function parameter count is zero or
	 *             runtime data value of the function parameter
	 * @return the function result
	 */
	@Override
	protected Object execute(Object data) {
		if (returnType == Attribute.Type.DOUBLE) {
			return Double.parseDouble(String.valueOf(data));
		} else {
			return Long.parseLong(String.valueOf(data));
		}
	}

	/**
	 * This will be called only once and this can be used to acquire
	 * required resources for the processing element.
	 * This will be called after initializing the system and before
	 * starting to process the events.
	 */
	@Override
	public void start() {

	}

	/**
	 * This will be called only once and this can be used to release
	 * the acquired resources for processing.
	 * This will be called before shutting down the system.
	 */
	@Override
	public void stop() {

	}

	@Override
	public Attribute.Type getReturnType() {
		return returnType;
	}

	/**
	 * Used to collect the serializable state of the processing element, that need to be
	 * persisted for the reconstructing the element to the same state on a different point of time
	 *
	 * @return stateful objects of the processing element as an array
	 */
	@Override
	public Object[] currentState() {
		return new Object[0];
	}

	/**
	 * Used to restore serialized state of the processing element, for reconstructing
	 * the element to the same state as if was on a previous point of time.
	 *
	 * @param state the stateful objects of the element as an array on
	 *              the same order provided by currentState().
	 */
	@Override
	public void restoreState(Object[] state) {

	}

}
