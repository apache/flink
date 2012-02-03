package eu.stratosphere.sopremo.serialization;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.sopremo.pact.JsonNodeWrapper;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IObjectNode;
import eu.stratosphere.sopremo.type.ObjectNode;

public class ObjectSchema implements Schema {

	/**
	 * @author Michael Hopstock
	 * @author Tommy Neubert
	 */
	private static final long serialVersionUID = 4037447354469753483L;

	List<String> mapping = new ArrayList<String>();
	
	@Override
	public Class<? extends Value>[] getPactSchema() {	
		Class<? extends Value>[] schema = new Class[this.mapping.size()];
		
		for(int i=0; i<this.mapping.size(); i++){
			schema[i] = JsonNodeWrapper.class;
		}
		
		return schema;
	}
	
	public void setMappings(String... schema) {
		this.mapping = Arrays.asList(schema);
	}

	@Override
	public PactRecord jsonToRecord(IJsonNode value, PactRecord target) {
		if(target == null){
			
			//the last element is the field "others"
			target = new PactRecord(this.mapping.size() + 1);
		}
		
		for(int i=0; i<this.mapping.size(); i++){
			target.setField(i, new JsonNodeWrapper(((IObjectNode)value).get(this.mapping.get(i))));
			((IObjectNode)value).remove(this.mapping.get(i));
		}
		
		target.setField(this.mapping.size(), new JsonNodeWrapper(value));
		
		return target;
	}

	@Override
	public IJsonNode recordToJson(PactRecord record, IJsonNode target) {
		if(this.mapping.size()+1 != record.getNumFields()){
			throw new IllegalStateException("Schema does not match to record!");
		}
		
		if(target == null){
			target = new ObjectNode();
		} else {
			((IObjectNode)target).removeAll();
		}
		
		for(int i=0; i< this.mapping.size(); i++){
			((IObjectNode)target).put(this.mapping.get(i), SopremoUtil.unwrap(record.getField(i, JsonNodeWrapper.class)));
		}
		
		((IObjectNode)target).putAll((IObjectNode)SopremoUtil.unwrap(record.getField(this.mapping.size(), JsonNodeWrapper.class)));	

		
		return target;
	}
	
}
