/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.streaming.api.invokable;

import java.io.Serializable;

import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.streaming.api.streamcomponent.StreamInvokableComponent;

public abstract class UserTaskInvokable<IN extends Tuple, OUT extends Tuple> extends StreamInvokableComponent<OUT> implements
		RecordInvokable<OUT>, Serializable {

	private static final long serialVersionUID = 1L;
}