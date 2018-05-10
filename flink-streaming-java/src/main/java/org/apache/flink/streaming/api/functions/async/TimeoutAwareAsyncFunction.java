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

package org.apache.flink.streaming.api.functions.async;

import org.apache.flink.annotation.PublicEvolving;

/**
 * An enhanced {@link AsyncFunction} which can handle timeouts.
 */
@PublicEvolving
public interface TimeoutAwareAsyncFunction<IN, OUT> extends AsyncFunction<IN, OUT> {

	/**
	 * asyncInvoke timeout occurred.
	 * Here you can complete the result future exceptionally with timeout exception,
	 * or complete with empty result. You can also retry to complete with the right results.
	 *
	 * @param input element coming from an upstream task
	 * @param resultFuture to be completed with the result data
	 * @exception Exception in case of a user code error. An exception will make the task fail and
	 * trigger fail-over process.
	 */
	void timeout(IN input, ResultFuture<OUT> resultFuture) throws Exception;

}
