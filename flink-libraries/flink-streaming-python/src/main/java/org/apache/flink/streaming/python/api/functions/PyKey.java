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

package org.apache.flink.streaming.python.api.functions;

/**
 * A key type used by {@link org.apache.flink.streaming.python.api.functions.PythonKeySelector} to
 * host a python object and provide the necessary interface to compare two python objects.
 * It is used internally by the python thin wrapper layer over the streaming data sets.
 */
public class PyKey {
	private Object data;

	public PyKey(Object data) {
		this.data = data;
	}

	public Object getData() {
		return data;
	}

	public void setData(Object data) {
		this.data = data;
	}

	@Override
	public boolean equals(Object other) {
		if (!(other instanceof PyKey)) {
			return false;
		}
		return (((PyKey) other).data.equals(this.data));
	}

	@Override
	public int hashCode() {
		return data.hashCode();
	}
}
