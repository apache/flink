/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.flink.api.common.functions;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Must be implemented by stoppable functions, eg, source functions of streaming jobs. The method {@link #stop()} will
 * be called when the job received the STOP signal. On this signal, the source function must stop emitting new data and
 * terminate gracefully.
 */
@PublicEvolving
public interface StoppableFunction {
	/**
	 * Stops the source. In contrast to {@code cancel()} this is a request to the source function to shut down
	 * gracefully. Pending data can still be emitted and it is not required to stop immediately -- however, in the near
	 * future. The job will keep running until all emitted data is processed completely.
	 *
	 * <p>Most streaming sources will have a while loop inside the {@code run()} method. You need to ensure that the source
	 * will break out of this loop. This can be achieved by having a volatile field "isRunning" that is checked in the
	 * loop and that is set to false in this method.
	 *
	 * <p><strong>The call to {@code stop()} should not block and not throw any exception.</strong>
	 */
	void stop();
}
