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
package org.apache.flink.streaming.connectors.rethinkdb;

import java.io.Serializable;

/**
 * Interface implemented by transformer that can convert an input into a {@link org.json.simple.JSONArray;} or {@link org.json.simple.JSONObject}

 * 
 * @param <T> the input type
 * 
 * @see StringJSONSerializationSchema
 */
public interface JSONSerializationSchema<T> extends Serializable{
	
	/**
	 * Transform input into JSON object
	 * @param input
	 * @return json array or object
	 * @throws Exception if there is a problem while transforming the object
	 */
	public Object toJSON(T input) throws Exception;
}
