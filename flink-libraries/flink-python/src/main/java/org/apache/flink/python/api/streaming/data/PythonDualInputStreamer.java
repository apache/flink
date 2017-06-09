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

package org.apache.flink.python.api.streaming.data;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.net.SocketTimeoutException;
import java.util.Iterator;

/**
 * This class is a {@link PythonStreamer} for operations with two input stream.
 *
 * @param <IN1> first input type
 * @param <IN2> second input type
 * @param <OUT> output type
 */
public class PythonDualInputStreamer<IN1, IN2, OUT> extends PythonStreamer<PythonDualInputSender<IN1, IN2>, OUT> {

	private static final long serialVersionUID = -607175070491761873L;

	public PythonDualInputStreamer(AbstractRichFunction function, Configuration config, int envID, int setID, boolean usesByteArray) {
		super(function, config, envID, setID, usesByteArray, new PythonDualInputSender<IN1, IN2>(config));
	}

	/**
	 * Sends all values contained in both iterators to the external process and collects all results.
	 *
	 * @param iterator1 first input stream
	 * @param iterator2 second input stream
	 * @param c         collector
	 */
	public final void streamBufferWithGroups(Iterator<IN1> iterator1, Iterator<IN2> iterator2, Collector<OUT> c) {
		SingleElementPushBackIterator<IN1> i1 = new SingleElementPushBackIterator<>(iterator1);
		SingleElementPushBackIterator<IN2> i2 = new SingleElementPushBackIterator<>(iterator2);
		try {
			int size;
			if (i1.hasNext() || i2.hasNext()) {
				while (true) {
					int sig = in.readInt();
					switch (sig) {
						case SIGNAL_BUFFER_REQUEST_G0:
							if (i1.hasNext()) {
								size = sender.sendBuffer1(i1);
								sendWriteNotification(size, i1.hasNext());
							}
							break;
						case SIGNAL_BUFFER_REQUEST_G1:
							if (i2.hasNext()) {
								size = sender.sendBuffer2(i2);
								sendWriteNotification(size, i2.hasNext());
							}
							break;
						case SIGNAL_FINISHED:
							return;
						case SIGNAL_ERROR:
							try {
								outPrinter.join();
							} catch (InterruptedException e) {
								outPrinter.interrupt();
							}
							try {
								errorPrinter.join();
							} catch (InterruptedException e) {
								errorPrinter.interrupt();
							}
							throw new RuntimeException(
								"External process for task " + function.getRuntimeContext().getTaskName() + " terminated prematurely due to an error." + msg);
						default:
							receiver.collectBuffer(c, sig);
							sendReadConfirmation();
							break;
					}
				}
			}
		} catch (SocketTimeoutException ignored) {
			throw new RuntimeException("External process for task " + function.getRuntimeContext().getTaskName() + " stopped responding." + msg);
		} catch (Exception e) {
			throw new RuntimeException("Critical failure for task " + function.getRuntimeContext().getTaskName() + ". " + msg.get(), e);
		}
	}
}
