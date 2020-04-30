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

package org.apache.flink.streaming.api.functions.source;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.MailboxExecutor;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.YieldingOperatorFactory;

/**
 * {@link ContinuousFileReaderOperator} factory.
 */
public class ContinuousFileReaderOperatorFactory<OUT> extends AbstractStreamOperatorFactory<OUT>
	implements YieldingOperatorFactory<OUT>, OneInputStreamOperatorFactory<TimestampedFileInputSplit, OUT> {

	private final FileInputFormat<OUT> inputFormat;
	private TypeInformation<OUT> type;
	private ExecutionConfig executionConfig;
	private transient MailboxExecutor mailboxExecutor;

	public ContinuousFileReaderOperatorFactory(FileInputFormat<OUT> inputFormat) {
		this(inputFormat, null, null);
	}

	public ContinuousFileReaderOperatorFactory(FileInputFormat<OUT> inputFormat, TypeInformation<OUT> type, ExecutionConfig executionConfig) {
		this.inputFormat = inputFormat;
		this.type = type;
		this.executionConfig = executionConfig;
		this.chainingStrategy = ChainingStrategy.HEAD;
	}

	@Override
	public void setMailboxExecutor(MailboxExecutor mailboxExecutor) {
		this.mailboxExecutor = mailboxExecutor;
	}

	@Override
	public <T extends StreamOperator<OUT>> T createStreamOperator(StreamOperatorParameters<OUT> parameters) {
		ContinuousFileReaderOperator<OUT> operator = new ContinuousFileReaderOperator<>(inputFormat, processingTimeService, mailboxExecutor);
		operator.setup(parameters.getContainingTask(), parameters.getStreamConfig(), parameters.getOutput());
		operator.setOutputType(type, executionConfig);
		return (T) operator;
	}

	@Override
	public void setOutputType(TypeInformation<OUT> type, ExecutionConfig executionConfig) {
		this.type = type;
		this.executionConfig = executionConfig;
	}

	@Override
	public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
		return ContinuousFileReaderOperator.class;
	}

	@Override
	public boolean isOutputTypeConfigurable() {
		return true;
	}
}
