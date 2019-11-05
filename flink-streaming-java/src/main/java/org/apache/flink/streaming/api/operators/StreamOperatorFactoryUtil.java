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

import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxExecutorFactory;

/**
 * A utility to instantiate new operators with a given factory.
 */
public class StreamOperatorFactoryUtil {
	/**
	 * Creates a new operator using a factory and makes sure that all special factory traits are properly handled.
	 *
	 * @param operatorFactory the operator factory.
	 * @param containingTask the containing task.
	 * @param configuration the configuration of the operator.
	 * @param output the output of the operator.
	 * @return a newly created and configured operator.
	 */
	public static <OUT, OP extends StreamOperator<OUT>> OP createOperator(
			StreamOperatorFactory<OUT> operatorFactory,
			StreamTask<OUT, ?> containingTask,
			StreamConfig configuration,
			Output<StreamRecord<OUT>> output) {
		MailboxExecutorFactory mailboxExecutorFactory = containingTask.getMailboxExecutorFactory();
		if (operatorFactory instanceof YieldingOperatorFactory) {
			MailboxExecutor mailboxExecutor = mailboxExecutorFactory.createExecutor(configuration.getChainIndex());
			((YieldingOperatorFactory) operatorFactory).setMailboxExecutor(mailboxExecutor);
		}
		return operatorFactory.createStreamOperator(containingTask, configuration, output);
	}
}
