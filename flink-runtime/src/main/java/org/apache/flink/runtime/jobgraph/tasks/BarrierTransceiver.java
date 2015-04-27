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

package org.apache.flink.runtime.jobgraph.tasks;


import java.io.IOException;

/**
 * A BarrierTransceiver describes an operator's barrier checkpointing behavior used for 
 * fault tolerance. In the most common case [[broadcastBarrier]] is being expected to be called 
 * periodically upon receiving a checkpoint barrier. Furthermore, a [[confirmBarrier]] method should
 * be implemented and used for acknowledging a specific checkpoint checkpoint.
 */
public interface BarrierTransceiver {

	/**
	 * A callback for notifying an operator of a new checkpoint barrier.
	 * @param barrierID
	 */
	public void broadcastBarrierFromSource(long barrierID);

	/**
	 * A callback for confirming that a barrier checkpoint is complete
	 * @param barrierID
	 */
	public void confirmBarrier(long barrierID) throws IOException;
	
}
