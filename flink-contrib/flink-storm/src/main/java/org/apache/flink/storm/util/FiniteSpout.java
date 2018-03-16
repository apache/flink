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

package org.apache.flink.storm.util;

import org.apache.storm.topology.IRichSpout;

/**
 * This interface represents a spout that emits a finite number of records. Common spouts emit infinite streams by
 * default. To change this behavior and take advantage of Flink's finite-source capabilities, the spout should implement
 * this interface.
 */
public interface FiniteSpout extends IRichSpout {

	/**
	 * When returns true, the spout has reached the end of the stream.
	 *
	 * @return true, if the spout's stream reached its end, false otherwise
	 */
	boolean reachedEnd();

}
