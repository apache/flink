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

package eu.stratosphere.nephele.event.task;

import eu.stratosphere.core.io.IOReadableWritable;

/**
 * This type of event can be used to exchange notification messages between
 * different {@link TaskManager} objects at runtime using the communication
 * channels Nephele has established between different tasks.
 * 
 */
public abstract class AbstractEvent implements IOReadableWritable {

}
