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

package org.apache.flink.api.java.io;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * This interface is the counterpart to the {@link RemoteCollectorOutputFormat}
 * and implementations will receive remote results through the collect function.
 * 
 * @param <T>
 *            The type of the records the collector will receive
 */
public interface RemoteCollector<T> extends Remote {

	public void collect(T element) throws RemoteException;

	public RemoteCollectorConsumer<T> getConsumer() throws RemoteException;

	public void setConsumer(RemoteCollectorConsumer<T> consumer)
			throws RemoteException;

}
