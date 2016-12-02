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

package org.apache.flink.streaming.connectors.activemq.internal;

import org.slf4j.Logger;

import javax.jms.ExceptionListener;
import javax.jms.JMSException;

public class AMQExceptionListener implements ExceptionListener {

	private final boolean logFailuresOnly;
	private final Logger logger;
	private JMSException exception;

	public AMQExceptionListener(Logger logger, boolean logFailuresOnly) {
		this.logger = logger;
		this.logFailuresOnly = logFailuresOnly;
	}

	@Override
	public void onException(JMSException e) {
		this.exception = e;
	}

	/**
	 * Check if the listener received an asynchronous exception. Throws an exception if it was
	 * received and if logFailuresOnly was set to true. Resets the state after the call
	 * so a single exception can be thrown only once.
	 *
	 * @throws JMSException if exception was received and logFailuresOnly was set to true.
	 */
	public void checkErroneous() throws JMSException {
		if (exception == null) {
			return;
		}

		JMSException recordedException = exception;
		exception = null;
		if (logFailuresOnly) {
			logger.error("Received ActiveMQ exception", recordedException);
		} else {
			throw recordedException;
		}
	}
}
