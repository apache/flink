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

package org.apache.flink.streaming.api.invokable.operator.co;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.invokable.StreamInvokable;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;
import org.apache.flink.streaming.api.streamrecord.StreamRecordSerializer;
import org.apache.flink.streaming.api.streamvertex.StreamTaskContext;
import org.apache.flink.streaming.io.CoReaderIterator;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class CoInvokable<IN1, IN2, OUT> extends StreamInvokable<IN1, OUT> {

	public CoInvokable(Function userFunction) {
		super(userFunction);
	}

	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(CoInvokable.class);

	protected CoReaderIterator<StreamRecord<IN1>, StreamRecord<IN2>> recordIterator;
	protected StreamRecord<IN1> reuse1;
	protected StreamRecord<IN2> reuse2;
	protected StreamRecordSerializer<IN1> srSerializer1;
	protected StreamRecordSerializer<IN2> srSerializer2;
	protected TypeSerializer<IN1> serializer1;
	protected TypeSerializer<IN2> serializer2;

	@Override
	public void setup(StreamTaskContext<OUT> taskContext, ExecutionConfig executionConfig) {
		this.collector = taskContext.getOutputCollector();

		this.recordIterator = taskContext.getCoReader();

		this.srSerializer1 = taskContext.getInputSerializer(0);
		this.srSerializer2 = taskContext.getInputSerializer(1);

		this.reuse1 = srSerializer1.createInstance();
		this.reuse2 = srSerializer2.createInstance();

		this.serializer1 = srSerializer1.getObjectSerializer();
		this.serializer2 = srSerializer2.getObjectSerializer();
	}

	protected void resetReuseAll() {
		this.reuse1 = srSerializer1.createInstance();
		this.reuse2 = srSerializer2.createInstance();
	}

	protected void resetReuse1() {
		this.reuse1 = srSerializer1.createInstance();
	}

	protected void resetReuse2() {
		this.reuse2 = srSerializer2.createInstance();
	}

	@Override
	public void invoke() throws Exception {
		while (true) {
			int next = recordIterator.next(reuse1, reuse2);
			if (next == 0) {
				break;
			} else if (next == 1) {
				initialize1();
				handleStream1();
				resetReuse1();
			} else {
				initialize2();
				handleStream2();
				resetReuse2();
			}
		}
	}

	protected abstract void handleStream1() throws Exception;

	protected abstract void handleStream2() throws Exception;

	protected abstract void callUserFunction1() throws Exception;

	protected abstract void callUserFunction2() throws Exception;

	protected void initialize1() {

	};

	protected void initialize2() {

	};

	protected void callUserFunctionAndLogException1() {
		try {
			callUserFunction1();
		} catch (Exception e) {
			if (LOG.isErrorEnabled()) {
				LOG.error("Calling user function failed due to: {}",
						StringUtils.stringifyException(e));
			}
		}
	}

	protected void callUserFunctionAndLogException2() {
		try {
			callUserFunction2();
		} catch (Exception e) {
			if (LOG.isErrorEnabled()) {
				LOG.error("Calling user function failed due to: {}",
						StringUtils.stringifyException(e));
			}
		}
	}

}
