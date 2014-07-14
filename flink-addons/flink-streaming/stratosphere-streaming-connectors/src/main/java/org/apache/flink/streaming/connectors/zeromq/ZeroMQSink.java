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

package org.apache.flink.streaming.connectors.zeromq;

import java.util.HashMap;

import org.apache.flink.streaming.api.function.SinkFunction;
import org.zeromq.*;

import org.apache.flink.api.java.tuple.Tuple;

public abstract class ZeroMQSink<IN extends Tuple> extends SinkFunction<IN> {
	private static final long serialVersionUID = 1L;
	
	private final String connection;
	
	private transient ZMQ.Context context;
	private transient ZMQ.Socket responder;
	private boolean initDone=false;
	private boolean close=false;
	private int socketType=0;
	
	HashMap<String, Integer> types = new HashMap<String, Integer>();
	
	public ZeroMQSink(String connection, String type) throws Exception{
		this.connection=connection;
		setType(type);
	}
	
	private void setType(String type) throws Exception{
		fillTypes();
		if(!types.containsKey(type)) throw new Exception(type + " is not a socketType option.");
		socketType = types.get(type);
	}
	
	private void fillTypes(){
		types.put("pubsub", 1);
		types.put("exclusive", 0);
		types.put("pushpull", 8);
	}
	
	public void initializeConnection(){
		context = ZMQ.context(1);
		responder = context.socket(socketType);
        responder.bind(connection);
        
        initDone = true;
	}
	
	
	@Override
	public void invoke(IN tuple) {
		if(!initDone) initializeConnection();
        responder.send(serialize(tuple), 0);
        if(close){
        	responder.close();
            context.term();
        }
	
	}
	
	public abstract byte[] serialize(Tuple t);
	public void close(){
		close=true;
	}

}
