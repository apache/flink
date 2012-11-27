/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.executiongraph;

/**
 * This interface allows to assign priorities to listener objects. If multiple listeners are registered for the same
 * object, the priority determines in which order they will be called.
 * 
 * @author warneke
 */
public interface PriorityListener {

	/**
	 * Returns the priority of the listener object. If multiple listener objects are registered for the same object, the
	 * priority determines in which order they will be called. Priorities are expressed as non-negative integer values.
	 * The lower the integer value, the higher the call priority.
	 * 
	 * @return the priority of this listener
	 */
	int getPriority();

}
