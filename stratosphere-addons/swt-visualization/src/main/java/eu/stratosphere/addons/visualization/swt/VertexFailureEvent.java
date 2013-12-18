/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.addons.visualization.swt;

/**
 * This class implements a vertex failure event which can be used to deliberately kill a particular task after the
 * given period of time.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 */
public final class VertexFailureEvent extends AbstractFailureEvent {

	/**
	 * Constructs a new vertex failure event
	 * 
	 * @param vertexName
	 *        the name of the vertex to be killed
	 * @param interval
	 *        the interval in milliseconds until this event will occur
	 */
	VertexFailureEvent(final String vertexName, final int interval) {
		super(vertexName, interval);

	}
}
