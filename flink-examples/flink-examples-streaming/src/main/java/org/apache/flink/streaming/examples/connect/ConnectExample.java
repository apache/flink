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

package org.apache.flink.streaming.examples.connect;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * Example illustrating connect between two data streams.
 *
 * <p>The example works on two input streams with pairs (id, money) and (id, cid-time)
 * respectively.  it connects two stream to fill Data
 *
 * <p>infosTable: order details table
 * ordersTable:   order table
 *
 */
public class ConnectExample {
	
	public static void main(String[] args) throws Exception {
		
		final List<Tuple2<String,String>> infosTable = new ArrayList<>();
		infosTable.add(new Tuple2("12001","10"));
		infosTable.add(new Tuple2("12002","25"));
		infosTable.add(new Tuple2("12003","36"));
		infosTable.add(new Tuple2("12004","47"));
		
		final List<Tuple2<String,String>> ordersTable = new ArrayList<>();
		ordersTable.add(new Tuple2<>("12001", "100-20190220"));
		ordersTable.add(new Tuple2<>("12002", "200-20190220"));
		ordersTable.add(new Tuple2<>("12003", "300-20190220"));
		ordersTable.add(new Tuple2<>("12004", "400-20190220"));
		ordersTable.add(new Tuple2<>("12005", "500-20190220"));
		
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		SingleOutputStreamOperator<Tuple2<String, String>> orders = env.fromCollection(ordersTable);
		SingleOutputStreamOperator<Tuple2<String, String>> infos = env.fromCollection(infosTable);
		
		ConnectedStreams<Tuple2<String, String>, Tuple2<String, String>> connect = infos.keyBy(0).connect(orders.keyBy(0));
		SingleOutputStreamOperator<Tuple2<String, String>> process = connect.process(new OrderMergeInfos());
		process.print("==========");
		
		env.execute();
	}
	
	
	public static class OrderMergeInfos extends CoProcessFunction<Tuple2<String, String>, Tuple2<String, String>, Tuple2<String, String>> {
		
		private MapState<String, String> infosMap = null;
		private MapState<String, String> ordersMap = null;
		
		@Override
		public void open(Configuration parameters){
			MapStateDescriptor infosDescriptor = new MapStateDescriptor<String, String>(
					"infosBuffer",
					BasicTypeInfo.STRING_TYPE_INFO,
					BasicTypeInfo.STRING_TYPE_INFO
			);
			infosMap = getRuntimeContext().getMapState(infosDescriptor);
			
			MapStateDescriptor ordersDescriptor = new MapStateDescriptor<String, String>(
					"ordersBuffer",
					BasicTypeInfo.STRING_TYPE_INFO,
					BasicTypeInfo.STRING_TYPE_INFO
			);
			ordersMap = getRuntimeContext().getMapState(ordersDescriptor);
		}
		
		@Override
		public void processElement1(Tuple2<String, String> value, Context ctx, Collector<Tuple2<String, String>> out) throws Exception {
			infosMap.put(value.f0,value.f1);
		}
		
		private Tuple2<String, String> outTuple2=new Tuple2<>();
		@Override
		public void processElement2(Tuple2<String, String> value, Context ctx, Collector<Tuple2<String, String>> out) throws Exception {
			ordersMap.put(value.f0,value.f1);
			for (Map.Entry<String,String> orders:ordersMap.entries()) {
				String infosCount = infosMap.get(orders.getKey());
				if(infosCount==null){
					ordersMap.put(value.f0,value.f1);
				}else {
					ordersMap.remove(orders.getKey());
					outTuple2.f0=value.f0;
					outTuple2.f1=value.f1+"-"+infosCount;
					out.collect(outTuple2);
				}
			}
		}
	}
}
