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

package org.apache.flink.streaming.api.functions.source;

/**
 * Base interface for all stream data sources in Flink. The contract of a stream source
 * is similar to an iterator - it is consumed as in the following pseudo code:
 * 
 * <pre>{@code
 * StreamSource<T> source = ...;
 * Collector<T> out = ...;
 * while (!source.reachedEnd()) {
 *   out.collect(source.next());
 * }
 * }
 * </pre>
 * 
 * <b>Note about blocking behavior</b>
 * <p>This implementations of the methods in the stream sources must have certain guarantees about
 * blocking behavior. One of the two characteristics must be fulfilled.</p>
 * <ul>
 *     <li>The methods must react to thread interrupt calls and break out of blocking calls with
 *         an {@link InterruptedException}.</li>
 *     <li>The method may ignore interrupt calls and/or swallow InterruptedExceptions, if it is guaranteed
 *         that the method returns quasi immediately irrespectively of the input. This is true for example
 *         for file streams, where the call is guaranteed to return after a very short I/O delay in
 *         the order of milliseconds.</li>
 * </ul>
 * 
 * @param <T> The type of the records produced by this source.
 */
public interface StreamSource<T> {
	
	/**
	 * Checks whether the stream has reached its end.
	 *
	 * <p>This method must obey the contract about blocking behavior declared in the
	 * description of this class.</p>
	 * 
	 * @return True, if the end of the stream has been reached, false if more data is available.
	 * 
	 * @throws InterruptedException The calling thread may be interrupted to pull the function out of this
	 *                              method during checkpoints.
	 * @throws Exception Any other exception that is thrown causes the source to fail and results in failure of
	 *                   the streaming program, or triggers recovery, depending on the program setup.
	 */
	boolean reachedEnd() throws Exception;


	/**
	 * Produces the next record.
	 * 
	 * <p>This method must obey the contract about blocking behavior declared in the
	 * description of this class.</p>
	 * 
	 * @return The next record produced by this stream source.
	 * 
	 * @throws InterruptedException The calling thread may be interrupted to pull the function out of this
	 *                              method during checkpoints.
	 * @throws Exception Any other exception that is thrown causes the source to fail and results in failure of
	 *                   the streaming program, or triggers recovery, depending on the program setup.
	 */
	T next() throws Exception;
}
