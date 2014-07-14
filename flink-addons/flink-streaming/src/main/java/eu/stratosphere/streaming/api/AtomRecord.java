package eu.stratosphere.streaming.api;

import eu.stratosphere.types.StringValue;
import eu.stratosphere.types.Value;

public class AtomRecord {
	private Value[] record;
	
	public AtomRecord(){
		record=new Value[1];
	}
	
	public AtomRecord(int length){
		record=new Value[length];
	}
	
	public AtomRecord(Value[] fields){
		record=fields;
	}
	
	public AtomRecord(Value fields) {
		record=new Value[1];
		record[0]=fields;
	}

	public Value[] getFields(){
		return record;
	}
	
	public Value getField(int fieldNumber){
		return record[fieldNumber];
	}
	
	public void setField(int fieldNumber, Value value){
		record[fieldNumber]=value;
	}
	
}
