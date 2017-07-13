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

package org.apache.flink.runtime.akka;

import akka.actor.ActorSystem;

/**
 * Callback interface for the {@link QuarantineMonitor} which is called in case the actor system
 * has been quarantined or quarantined another system.
 */
public interface QuarantineHandler {

	/**
	 * Callback when the given actor system was quarantined by the given remote actor system.
	 *
	 * @param remoteSystem is the address of the remote actor system which has quarantined this
	 *                     actor system
	 * @param actorSystem which has been quarantined
	 */
	void wasQuarantinedBy(String remoteSystem, ActorSystem actorSystem);

	/**
	 * Callback when the given actor system has quarantined the given remote actor system.
	 *
	 * @param remoteSystem is the address of the remote actor system which has been quarantined
	 *                     by our actor system
	 * @param actorSystem which has quarantined the other actor system
	 */
	void hasQuarantined(String remoteSystem, ActorSystem actorSystem);
}
