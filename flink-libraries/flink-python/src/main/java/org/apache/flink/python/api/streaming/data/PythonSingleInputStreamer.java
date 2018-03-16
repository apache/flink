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
 * This class is a {@link PythonStreamer} for operations with one input stream.
 * @param <IN> input type
 * @param <OUT> output type
 */
public class PythonSingleInputStreamer<IN, OUT> extends PythonStreamer<PythonSingleInputSender<IN>, OUT> {

	private static final long serialVersionUID = -5149905918522069034L;

	public PythonSingleInputStreamer(AbstractRichFunction function, Configuration config, int envID, int setID, boolean usesByteArray) {
		super(function, config, envID, setID, usesByteArray, new PythonSingleInputSender<IN>(config));
	}

	/**
	 * Sends all values contained in the iterator to the external process and collects all results.
	 *
	 * @param iterator input stream
	 * @param c        collector
	 */
	public final void streamBufferWithoutGroups(Iterator<IN> iterator, Collector<OUT> c) {
		SingleElementPushBackIterator<IN> i = new SingleElementPushBackIterator<>(iterator);
		try {
			int size;
			if (i.hasNext()) {
				while (true) {
					int sig = in.readInt();
					switch (sig) {
						case SIGNAL_BUFFER_REQUEST:
							if (i.hasNext()) {
								size = sender.sendBuffer(i);
								sendWriteNotification(size, i.hasNext());
							} else {
								throw new RuntimeException("External process requested data even though none is available.");
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
			throw new RuntimeException("External process for task " + function.getRuntimeContext().getTaskName() + " stopped responding." + msg.get());
		} catch (Exception e) {
			throw new RuntimeException("Critical failure for task " + function.getRuntimeContext().getTaskName() + ". " + msg.get(), e);
		}
	}
}
