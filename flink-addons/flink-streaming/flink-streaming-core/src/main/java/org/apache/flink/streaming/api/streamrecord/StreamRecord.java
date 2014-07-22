/**
 *
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
 *
 */

package org.apache.flink.streaming.api.streamrecord;

import java.io.Serializable;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;

/**
 * Object for wrapping a tuple with ID used for sending records between
 * streaming task in Apache Flink stream processing.
 */
public class StreamRecord<T extends Tuple> implements Serializable {
	private static final long serialVersionUID = 1L;

	private UID uid;
	private T tuple;

	protected TupleSerializer<T> tupleSerializer;

	public StreamRecord() {
		uid = new UID();
	}

	/**
	 * @return The ID of the object
	 */
	public UID getId() {
		return uid;
	}

	/**
	 * Creates a new ID for the StreamRecord using the given channelID
	 * 
	 * @param channelID
	 *            ID of the emitting task
	 * @return The StreamRecord object
	 */
	public StreamRecord<T> setId(int channelID) {
		uid = new UID(channelID);
		return this;
	}

	/**
	 * 
	 * @return The tuple contained
	 */
	public T getTuple() {
		return tuple;
	}

	/**
	 * Sets the tuple stored
	 * 
	 * @param tuple
	 *            Value to set
	 * @return Returns the StreamRecord object
	 */
	public StreamRecord<T> setTuple(T tuple) {
		this.tuple = tuple;
		return this;
	}

	@Override
	public String toString() {
		return tuple.toString();
	}

}
