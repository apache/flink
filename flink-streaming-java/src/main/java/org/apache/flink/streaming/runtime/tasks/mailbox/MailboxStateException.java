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

package org.apache.flink.streaming.runtime.tasks.mailbox;

/**
 * This exception signals that a method of the mailbox was invoked in a state that does not support the invocation,
 * e.g. on the attempt to put a letter into a closed mailbox.
 */
public class MailboxStateException extends Exception {

	MailboxStateException() {
	}

	MailboxStateException(String message) {
		super(message);
	}

	MailboxStateException(String message, Throwable cause) {
		super(message, cause);
	}

	MailboxStateException(Throwable cause) {
		super(cause);
	}
}
