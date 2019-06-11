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

package org.apache.flink.runtime.state;

/**
 * A key scope determines the keyed state is local or global.
 * Different scope means different aggregation mechanism. {@link KeyScope#GLOBAL} is used for
 * general aggregation based on hash partition. {@link KeyScope#LOCAL} is used for
 * local pre-aggregation.
 */
public enum KeyScope {

	GLOBAL(false),
	LOCAL(true);

	private final boolean local;

	KeyScope(boolean local) {
		this.local = local;
	}

	public boolean isLocal() {
		return local;
	}

}
