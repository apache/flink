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

package org.apache.flink.graph.translate;

import java.io.Serializable;

/**
 * A {@code Translator} provides a modular mapping of values.
 * <br/>
 * Possible translations include translating the value type and modifying
 * the value data.
 *
 * @param <OLD> old type
 * @param <NEW> new type
 *
 * @see Translate
 */
public interface Translator<OLD, NEW>
extends Serializable {

	/**
	 * Translate a value between types.
	 *
	 * @param value old value
	 * @param reuse optionally reuseable value; if null then create
	 *				and return a new object
	 * @return new value
	 */
	NEW translate(OLD value, NEW reuse);
}
