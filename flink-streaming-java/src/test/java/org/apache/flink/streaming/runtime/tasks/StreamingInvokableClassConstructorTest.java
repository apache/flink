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

package org.apache.flink.streaming.runtime.tasks;


import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.state.TaskStateHandles;
import org.junit.Test;

import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;

public class StreamingInvokableClassConstructorTest {

	private Environment dummyEnvironment = new DummyEnvironment("test", 1, 0);

	private TaskStateHandles dummyTaskStateHandles = TaskStateHandles.EMPTY;

	private String classNames[] = {
		OneInputStreamTask.class.getName(),
		TwoInputStreamTask.class.getName(),
		SourceStreamTask.class.getName(),
		StoppableSourceStreamTask.class.getName(),
		StreamIterationHead.class.getName(),
		StreamIterationTail.class.getName()
	};

	// ------------------------------------------------------------------------
	//  Tests
	// ------------------------------------------------------------------------

	@Test
	public void testConstructor() {
		try {
			for (String className: classNames) {
				AbstractInvokable invokable = createInvokableClass(className, dummyEnvironment, dummyTaskStateHandles);
				assertEquals(dummyEnvironment, invokable.getEnvironment());
				assertEquals(dummyTaskStateHandles, invokable.getTaskStateHandles());
			}
		}
		catch (Exception e){
			e.printStackTrace();
			fail(e.getMessage());
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
