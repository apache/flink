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

package org.apache.flink.streaming.api.windowing.policy;


/**
 * When used in grouped windowing, trigger policies can provide
 * a clone method. Cloneable triggers can be used in a distributed manner,
 * which means they get cloned to provide an own instance for each group. This
 * allows each group to trigger individually and only based on the elements
 * belonging to the respective group.
 * 
 * This interface extends {@link TriggerPolicy} with such a clone method. It
 * also adds the Java {@link Cloneable} interface as flag.
 * 
 * @param <DATA>
 *            The data type handled by this policy
 */
public interface CloneableTriggerPolicy<DATA> extends TriggerPolicy<DATA>, Cloneable {

	/**
	 * This method should return an exact copy of the object it belongs to
	 * including the current object state.
	 * 
	 * @return a copy of this object
	 */
	public CloneableTriggerPolicy<DATA> clone();

}
