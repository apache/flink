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

package org.apache.flink.streaming.connectors.activemq;

import org.apache.flink.streaming.connectors.activemq.internal.AMQExceptionListener;
import org.junit.Test;
import org.slf4j.Logger;

import javax.jms.JMSException;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class AMQExceptionListenerTest {
	@Test
	public void logMessageOnException() throws JMSException {
		Logger logger = mock(Logger.class);
		AMQExceptionListener listener = new AMQExceptionListener(logger, true);
		JMSException exception = new JMSException("error");
		listener.onException(exception);
		listener.checkErroneous();
		verify(logger).error("Received ActiveMQ exception", exception);
	}

	@Test
	public void logMessageWrittenOnlyOnce() throws JMSException {
		Logger logger = mock(Logger.class);
		AMQExceptionListener listener = new AMQExceptionListener(logger, true);
		JMSException exception = new JMSException("error");
		listener.onException(exception);
		listener.checkErroneous();
		listener.checkErroneous();
		verify(logger, times(1)).error("Received ActiveMQ exception", exception);
	}

	@Test(expected = JMSException.class)
	public void throwException() throws JMSException {
		Logger logger = mock(Logger.class);
		AMQExceptionListener listener = new AMQExceptionListener(logger, false);
		listener.onException(new JMSException("error"));
		listener.checkErroneous();
	}

	@Test
	public void throwExceptionOnlyOnce() throws JMSException {
		Logger logger = mock(Logger.class);
		AMQExceptionListener listener = new AMQExceptionListener(logger, false);
		listener.onException(new JMSException("error"));

		try {listener.checkErroneous();} catch (JMSException ignore) {}
		listener.checkErroneous();
	}

	@Test
	public void logMessageNotWrittenIfNoException() throws JMSException {
		Logger logger = mock(Logger.class);
		AMQExceptionListener listener = new AMQExceptionListener(logger, false);
		listener.checkErroneous();
		verify(logger, times(0)).error(any(String.class), any(Throwable.class));
	}
}
