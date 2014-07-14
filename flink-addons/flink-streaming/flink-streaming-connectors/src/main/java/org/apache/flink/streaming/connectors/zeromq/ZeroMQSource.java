/***********************************************************************************************************************
 *
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
 *
 **********************************************************************************************************************/

package org.apache.flink.streaming.connectors.zeromq;

import java.util.HashMap;

import org.apache.flink.streaming.api.function.SourceFunction;
import org.zeromq.*;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.util.Collector;

public abstract class ZeroMQSource<IN extends Tuple> extends SourceFunction<IN> {
	private static final long serialVersionUID = 1L;
	
	private final String connection;
	
	private int socketType;
	private String filter;
	HashMap<String, Integer> types = new HashMap<String, Integer>();
	private boolean close=false;
	IN outTuple;
	
	public ZeroMQSource(String connection) throws Exception{
		this.connection=connection;
	}
	public ZeroMQSource(String connection, String type) throws Exception{
		this.connection=connection;
		setType(type);
	}
	
	public ZeroMQSource(String connection, String type, String filter) throws Exception{
		this.connection=connection;
		setType(type);
		this.filter=filter;
	}
	
	private void setType(String type) throws Exception{
		fillTypes();
		if(!types.containsKey(type)) throw new Exception(type + " is not a socketType option.");
		socketType = types.get(type);
	}
	
	private void fillTypes(){
		types.put("pubsub", 2);
		types.put("exclusive", 0);
		types.put("pushpull", 7);
	}
	
	@Override
	public void invoke(Collector<IN> collector) throws Exception {
		ZMQ.Context context = ZMQ.context(1);
		

	    ZMQ.Socket requester = context.socket(socketType);
	    requester.connect(connection);
	    if(socketType==2){
	    	requester.subscribe(filter.getBytes());
	    }
	    while(true){
	        
	        outTuple=deserialize(requester.recv());
	        
	        collector.collect(outTuple);
	        if(close) break;
	       
	    }
	    requester.close();
	    context.term();
		
	}
	
	public abstract IN deserialize(byte[] t);
	
	public void close(){
		close=true;
	}
	
}
