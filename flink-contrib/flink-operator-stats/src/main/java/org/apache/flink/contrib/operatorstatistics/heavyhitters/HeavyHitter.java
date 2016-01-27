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

package org.apache.flink.contrib.operatorstatistics.heavyhitters;

import java.util.HashMap;

/**
 * Interface for classes that track heavy hitters. It follows the same design as the interfaces from
 * {@link com.clearspring.analytics.stream}
 */
public interface HeavyHitter {

	void addObject(Object o);

	HashMap getHeavyHitters();

	void merge(HeavyHitter toMerge) throws HeavyHitterMergeException;

	String toString();

}
