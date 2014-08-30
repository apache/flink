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

package org.apache.flink.streaming.api.streamrecord;

import java.io.Serializable;

import org.apache.flink.api.java.tuple.Tuple;

/**
 * Object for wrapping a tuple or other object with ID used for sending records
 * between streaming task in Apache Flink stream processing.
 */
public class StreamRecord<T> implements Serializable {
	private static final long serialVersionUID = 1L;

	private UID uid;
	private T streamObject;
	public boolean isTuple;

	/**
	 * Creates an empty StreamRecord
	 */
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
	public StreamRecord<T> newId(int channelID) {
		uid = new UID(channelID);
		return this;
	}

	/**
	 * Sets the ID of the StreamRecord
	 * 
	 * @param id
	 *            id to set
	 */
	public void setId(UID id) {
		this.uid = id;
	}

	/**
	 * Gets the wrapped object from the StreamRecord
	 * 
	 * @return The object wrapped
	 */
	public T getObject() {
		return streamObject;
	}

	/**
	 * Gets the field of the contained object at the given position. If a tuple
	 * is wrapped then the getField method is invoked. If the StreamRecord
	 * contains and object of Basic types only position 0 could be returned.
	 * 
	 * @param pos
	 *            Position of the field to get.
	 * @return Returns the object contained in the position.
	 */
	public Object getField(int pos) {
		if (isTuple) {
			return ((Tuple) streamObject).getField(pos);
		} else {
			if (pos == 0) {
				return streamObject;
			} else {
				throw new IndexOutOfBoundsException();
			}
		}
	}

	/**
	 * Sets the object stored
	 * 
	 * @param object
	 *            Object to set
	 * @return Returns the StreamRecord object
	 */
	public StreamRecord<T> setObject(T object) {
		this.streamObject = object;
		return this;
	}

	@Override
	public String toString() {
		return streamObject.toString();
	}

}
