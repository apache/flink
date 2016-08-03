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

package org.apache.flink.runtime.rpc.akka;

import akka.actor.Status;
import akka.actor.UntypedActor;
import org.apache.flink.runtime.rpc.akka.messages.CallableMessage;
import org.apache.flink.runtime.rpc.akka.messages.RunnableMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseAkkaActor extends UntypedActor {
	private static final Logger LOG = LoggerFactory.getLogger(BaseAkkaActor.class);

	@Override
	public void onReceive(Object message) throws Exception {
		if (message instanceof RunnableMessage) {
			try {
				((RunnableMessage) message).getRunnable().run();
			} catch (Exception e) {
				LOG.error("Encountered error while executing runnable.", e);
			}
		} else if (message instanceof CallableMessage<?>) {
			try {
				Object result = ((CallableMessage<?>) message).getCallable().call();
				sender().tell(new Status.Success(result), getSelf());
			} catch (Exception e) {
				sender().tell(new Status.Failure(e), getSelf());
			}
		} else {
			throw new RuntimeException("Unknown message " + message);
		}
	}
}
