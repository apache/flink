/**
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.streaming.api.invokable.StreamInvokable;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;
import org.apache.flink.streaming.api.streamrecord.StreamRecordSerializer;
import org.apache.flink.streaming.io.CoReaderIterator;
import org.apache.flink.util.Collector;
import org.apache.flink.util.StringUtils;

public abstract class CoInvokable<IN1, IN2, OUT> extends StreamInvokable<OUT> {

	public CoInvokable(Function userFunction) {
		super(userFunction);
	}

	private static final long serialVersionUID = 1L;
	private static final Log LOG = LogFactory.getLog(CoInvokable.class);

	protected CoReaderIterator<StreamRecord<IN1>, StreamRecord<IN2>> recordIterator;
	protected StreamRecord<IN1> reuse1;
	protected StreamRecord<IN2> reuse2;
	protected StreamRecordSerializer<IN1> serializer1;
	protected StreamRecordSerializer<IN2> serializer2;
	protected boolean isMutable;

	public void initialize(Collector<OUT> collector,
			CoReaderIterator<StreamRecord<IN1>, StreamRecord<IN2>> recordIterator,
			StreamRecordSerializer<IN1> serializer1, StreamRecordSerializer<IN2> serializer2,
			boolean isMutable) {
		this.collector = collector;

		this.recordIterator = recordIterator;
		this.reuse1 = serializer1.createInstance();
		this.reuse2 = serializer2.createInstance();

		this.serializer1 = serializer1;
		this.serializer2 = serializer2;
		this.isMutable = isMutable;
	}

	protected void resetReuseAll() {
		this.reuse1 = serializer1.createInstance();
		this.reuse2 = serializer2.createInstance();
	}

	protected void resetReuse1() {
		this.reuse1 = serializer1.createInstance();
	}

	protected void resetReuse2() {
		this.reuse2 = serializer2.createInstance();
	}

	public void invoke() throws Exception {
		if (this.isMutable) {
			mutableInvoke();
		} else {
			immutableInvoke();
		}
	}

	protected void immutableInvoke() throws Exception {
		while (true) {
			int next = recordIterator.next(reuse1, reuse2);
			if (next == 0) {
				break;
			} else if (next == 1) {
				handleStream1();
				resetReuse1();
			} else {
				handleStream2();
				resetReuse2();
			}
		}
	}

	protected void mutableInvoke() throws Exception {
		while (true) {
			int next = recordIterator.next(reuse1, reuse2);
			if (next == 0) {
				break;
			} else if (next == 1) {
				handleStream1();
			} else {
				handleStream2();
			}
		}
	}

	protected abstract void handleStream1() throws Exception;

	protected abstract void handleStream2() throws Exception;

	protected abstract void callUserFunction1() throws Exception;

	protected abstract void callUserFunction2() throws Exception;
	
	protected void callUserFunctionAndLogException1() {
		try {
			callUserFunction1();
		} catch (Exception e) {
			if (LOG.isErrorEnabled()) {
				LOG.error(String.format("Calling user function failed due to: %s",
						StringUtils.stringifyException(e)));
			}
		}
	}

	protected void callUserFunctionAndLogException2() {
		try {
			callUserFunction2();
		} catch (Exception e) {
			if (LOG.isErrorEnabled()) {
				LOG.error(String.format("Calling user function failed due to: %s",
						StringUtils.stringifyException(e)));
			}
		}
	}

}
