/**
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

package org.apache.flink.compiler.postpass;

import java.util.Map;

public abstract class AbstractSchema<X> implements Iterable<Map.Entry<Integer, X>> {

	private int numConnectionsThatContributed;
	
	
	public int getNumConnectionsThatContributed() {
		return this.numConnectionsThatContributed;
	}
	
	public void increaseNumConnectionsThatContributed() {
		this.numConnectionsThatContributed++;
	}
	
	public abstract void addType(int pos, X type) throws ConflictingFieldTypeInfoException;
	
	public abstract X getType(int field);
}
