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
package org.apache.flink.streaming.connectors.rethinkdb.integration;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

/**
 * Test utility to generate json events for the IntegrationTest.  This class
 * produces both JSONObject and JSONArray for testing the sink. The JSONObjects
 * can have an <code>id</code> field based on the {@link #addId} flag.
 */
public class JSONEventGenerator implements SourceFunction<String> {
	
	/**
	 * Last name field for json
	 */
	private static final String LAST = "last";

	/**
	 * First name field for json
	 */
	private static final String FIRST = "first";

	/**
	 * name field for json
	 */
	private static final String NAME = "name";

	/**
	 * Id field for json
	 */
	private static final String ID = "id";

	private boolean addId = true;
	
	/**
	 * Constructor 
	 * @param addId - whether to add an id field to the json objects or not
	 * The "id" field is default id for RethinkDB documents
	 */
	public JSONEventGenerator(boolean addId) {
		this.addId = addId;
	}
	/**
	 * Serial version
	 */
	private static final long serialVersionUID = 1L;
	
	/**
	 * Flag to indicate the source is running
	 */
	private boolean running = true;

	/**
	 * Emit events until {@link #cancel}cancel is called
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void run(SourceContext<String> ctx) throws Exception {
		long seqSuffix = 0;
		long loops = 0;
		while(running) {
			Thread.sleep(10);
			
			seqSuffix++;
			JSONObject json1 = new JSONObject();
			if ( addId )
				json1.put(ID, "1" + seqSuffix);
			json1.put(NAME, "john stu " + seqSuffix);
			json1.put(FIRST, "john " + seqSuffix);
			json1.put(LAST, "stu "+ seqSuffix);
			
			seqSuffix++;
			JSONObject json2 = new JSONObject();
			if ( addId )
				json2.put(ID, "2" + seqSuffix);
			json2.put(NAME, "jane doe"+ seqSuffix);
			json2.put(FIRST, "jane"+ seqSuffix);
			json2.put(LAST, "doe"+ seqSuffix);
			JSONArray array = new JSONArray();
			loops++;
			if ( loops % 2 == 0 )
				array.add(json1);
			if ( loops % 3 == 0)
				array.add(json2);
			
			if ( array.size() != 0 )
				ctx.collect(array.toString());
			else 
				ctx.collect(json1.toString());
		}
	}

	/**
	 * Cancel producing events
	 */
	@Override
	public void cancel() {
		running = false;
	}
	
}
