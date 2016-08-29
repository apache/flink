package org.apache.flink.contrib.siddhi.operator;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;

import java.util.HashMap;
import java.util.Map;

public class StreamOutputHandler<R> extends StreamCallback {
	private static final Logger LOGGER = LoggerFactory.getLogger(StreamOutputHandler.class);

	private final AbstractDefinition definition;
	private final Output<StreamRecord<R>> output;
	private final TypeInformation<R> typeInfo;
	private final ObjectMapper objectMapper;

	public StreamOutputHandler(TypeInformation<R> typeInfo, AbstractDefinition definition, Output<StreamRecord<R>> output) {
		this.typeInfo = typeInfo;
		this.definition = definition;
		this.output = output;
		this.objectMapper = new ObjectMapper();
		this.objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
	}

	@Override @SuppressWarnings("unchecked")
	public void receive(Event[] events) {
		for(Event event:events){
			if(typeInfo == null || typeInfo.getTypeClass().equals(Map.class)){
				StreamRecord<R> record = new StreamRecord<>((R) toMap(event));
				record.setTimestamp(event.getTimestamp());
				output.collect(record);
			} else if(typeInfo.isTupleType()){
				StreamRecord<R> record = new StreamRecord<>((R) toTuple(event));
				record.setTimestamp(event.getTimestamp());
				output.collect(record);
			} else if (typeInfo instanceof PojoTypeInfo){
				R obj;
				try {
					obj = objectMapper.convertValue(toMap(event), typeInfo.getTypeClass());
				} catch (IllegalArgumentException ex){
					LOGGER.error("Failed to map event: "+event+" into type: "+typeInfo,ex);
					throw ex;
				}
				StreamRecord<R> record = new StreamRecord<>(obj);
				record.setTimestamp(event.getTimestamp());
				output.collect(record);
			} else {
				throw new IllegalArgumentException("Unable to format "+event+" as type "+typeInfo);
			}
		}
		output.collect(new StreamRecord<R>(null));
	}

	private Map<String,Object> toMap(Event event){
		Map<String,Object> map = new HashMap<>();
		for(int i=0;i< definition.getAttributeNameArray().length;i++){
			map.put(definition.getAttributeNameArray()[i],event.getData(i));
		}
		return map;
	}

	private Tuple toTuple(Event event){
		Object[] row = event.getData();
		switch (row.length){
			case 0:
				return new Tuple0();
			case 1:
				return Tuple1.of(row[0]);
			case 2:
				return Tuple2.of(row[0],row[1]);
			case 3:
				return Tuple3.of(row[0],row[1],row[2]);
			case 4:
				return Tuple4.of(row[0],row[1],row[2],row[3]);
			case 5:
				return Tuple5.of(row[0],row[1],row[2],row[3],row[4]);
			case 6:
				return Tuple6.of(row[0],row[1],row[2],row[3],row[4],row[5]);
			case 7:
				return Tuple7.of(row[0],row[1],row[2],row[3],row[4],row[5],row[6]);
			case 8:
				return Tuple8.of(row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7]);
			case 9:
				return Tuple9.of(row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8]);
			case 10:
				return Tuple10.of(row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9]);
			default:
				throw new IllegalArgumentException("Unable to convert event to tuple in size > 10: "+event);
		}
	}
}
