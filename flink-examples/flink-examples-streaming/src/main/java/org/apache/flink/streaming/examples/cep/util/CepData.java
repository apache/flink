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

package org.apache.flink.streaming.examples.cep.util;

/**
 * Created by growingio on 2020-05-15.
 */
public class CepData {

	public static Event[] CEP_EVENTS = {
		new Event(41, 5.0, "init"),
		new Event(42, 5.0, "start"),
		new SubEvent(43, 10.0, "middle"),
		new Event(44, 10.0, "end")
	};
}
