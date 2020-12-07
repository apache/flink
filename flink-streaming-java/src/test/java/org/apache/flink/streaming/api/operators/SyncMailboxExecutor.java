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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.function.ThrowingRunnable;

/**
 * A mailbox executor that immediately executes code in the current thread.
 */
public class SyncMailboxExecutor implements MailboxExecutor {
	@Override
	public void execute(ThrowingRunnable<? extends Exception> command, String descriptionFormat, Object... descriptionArgs) {
		try {
			command.run();
		} catch (Exception e) {
			throw new FlinkRuntimeException("Cannot execute mail " + String.format(descriptionFormat, descriptionArgs), e);
		}
	}

	@Override
	public void yield() throws FlinkRuntimeException {
	}

	@Override
	public boolean tryYield() throws FlinkRuntimeException {
		return false;
	}
}
