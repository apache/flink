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

package org.apache.flink.runtime.taskmanager;

import org.apache.flink.runtime.instance.BaseTestingActorGateway;
import org.apache.flink.runtime.testingUtils.TestingUtils;

import java.util.concurrent.BlockingQueue;

/**
 * Testing {@link org.apache.flink.runtime.instance.ActorGateway} which stores all handled messages
 * in a {@link BlockingQueue}.
 */
public class ForwardingActorGateway extends BaseTestingActorGateway {
	private static final long serialVersionUID = 7001973884263802603L;

	private final transient BlockingQueue<Object> queue;

	public ForwardingActorGateway(BlockingQueue<Object> queue) {
		super(TestingUtils.directExecutionContext());

		this.queue = queue;
	}

	@Override
	public Object handleMessage(Object message) throws Exception {
		// don't do anything with the message, but storing it in queue
		queue.add(message);

		return null;
	}
}
