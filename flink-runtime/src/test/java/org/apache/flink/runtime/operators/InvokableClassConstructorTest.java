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

package org.apache.flink.runtime.operators;


import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.iterative.task.IterationHeadTask;
import org.apache.flink.runtime.iterative.task.IterationIntermediateTask;
import org.apache.flink.runtime.iterative.task.IterationSynchronizationSinkTask;
import org.apache.flink.runtime.iterative.task.IterationTailTask;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.state.TaskStateHandles;
import org.junit.Test;

import static org.hamcrest.core.IsInstanceOf.instanceOf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class InvokableClassConstructorTest {

	private Environment dummyEnvironment = new DummyEnvironment("test", 1, 0);

	private TaskStateHandles dummyTaskStateHandles = TaskStateHandles.EMPTY;

	private String classNames[] = {
		IterationHeadTask.class.getName(),
		IterationIntermediateTask.class.getName(),
		IterationTailTask.class.getName(),
		IterationSynchronizationSinkTask.class.getName(),
		DataSourceTask.class.getName(),
		DataSinkTask.class.getName()
	};

	// ------------------------------------------------------------------------
	//  Tests
	// ------------------------------------------------------------------------

	@Test
	public void testConstructor() {
		try {
			for (String className: classNames) {
				AbstractInvokable iterationHeadTask = createInvokableClass(className, dummyEnvironment, null);
				assertEquals(dummyEnvironment, iterationHeadTask.getEnvironment());
				assertEquals(null, iterationHeadTask.getTaskStateHandles());
			}
		}
		catch (Exception e){
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testIllegalParameterInConstructor() {
		String illegalStateExceptionMessage = "Found operator state for a non-stateful task invokable";

		for (String className: classNames) {
			try {
				createInvokableClass(className, dummyEnvironment, dummyTaskStateHandles);
			}
			catch (Exception e) {
				assertThat(e.getCause(), instanceOf(IllegalStateException.class));
				assertEquals(illegalStateExceptionMessage, e.getCause().getMessage());
			}
		}
	}

	private AbstractInvokable createInvokableClass(String className,
								 					Environment environment,
								 					TaskStateHandles taskStateHandles) throws Exception {
		Class<? extends AbstractInvokable> invokableClass;
		try {
			invokableClass = Class.forName(className, true, getClass().getClassLoader())
				.asSubclass(AbstractInvokable.class);
		}
		catch (Throwable t) {
			throw new Exception("Could not load the task's invokable class.", t);
		}

		return invokableClass.getConstructor(Environment.class, TaskStateHandles.class).newInstance(environment, taskStateHandles);
	}
}
