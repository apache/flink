/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.api.windowing.triggers.triggerdsl;

/**
 * Describes if the trigger is to fire once for a given key and window and then stop, or
 * fire repeatedly, throughout the lifetime of the window. By default, triggers are <tt>Repeated.ONCE</tt>.
 * To change this, use the {@link Repeat#Forever(DslTrigger)}, as in the following example:
 * <pre>
 *     {@code Repeat.Forever(EventTime.<TimeWindow>afterFirstElement(Time.milliseconds(100)))
 * }</pre>
 * The example above will fire every 100 milliseconds (in event time).
 */
public enum Repeated {

	ONCE(false),

	FOREVER(true),

	UNDEFINED(null);

	private final Boolean isRepeated;

	Repeated(Boolean fireRepeatedly) {
		this.isRepeated = fireRepeatedly;
	}

	public boolean isUndefined() {
		return isRepeated == null;
	}

	public boolean once() {
		return isRepeated != null && !isRepeated;
	}

	public boolean forever() {
		return isRepeated != null && isRepeated;
	}
}
