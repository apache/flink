/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks.mailbox.execution;

import org.apache.flink.annotation.Internal;

/**
 * Interface for the default action that is repeatedly invoked in the mailbox-loop.
 */
@Internal
public interface MailboxDefaultAction {

	/**
	 * This method implements the default action of the mailbox loop (e.g. processing one event from the input).
	 * Implementations should (in general) be non-blocking.
	 *
	 * @param context context object for collaborative interaction between the default action and the mailbox loop.
	 * @throws Exception on any problems in the action.
	 */
	void runDefaultAction(DefaultActionContext context) throws Exception;
}
