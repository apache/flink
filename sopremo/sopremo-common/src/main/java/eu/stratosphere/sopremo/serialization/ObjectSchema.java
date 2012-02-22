package eu.stratosphere.sopremo.serialization;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;

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

	private List<String> mapping = new ArrayList<String>();
	
	@Override
	public Class<? extends Value>[] getPactSchema() {	
		Class<? extends Value>[] schema = new Class[this.mapping.size()];
		
		for(int i=0; i<this.mapping.size(); i++){
			schema[i] = JsonNodeWrapper.class;
		}
		
		return schema;
	}
	
	public List<String> getMappings(){
		return this.mapping;
	}
	
	public void setMappings(String... schema) {
		this.mapping = Arrays.asList(schema);
	}
	
	public int hasMapping(String key){
		return this.mapping.indexOf(key);
	}
	
	public int getMappingSize(){
		return this.mapping.size();
	}

	@Override
	public PactRecord jsonToRecord(IJsonNode value, PactRecord target) {
		IObjectNode others;
		if(target == null){
			
			//the last element is the field "others"
			target = new PactRecord(this.mapping.size() + 1);
			others = new ObjectNode();
			target.setField(this.mapping.size(), new JsonNodeWrapper(others));
		} else {
			others = (IObjectNode)SopremoUtil.unwrap(target.getField(target.getNumFields() - 1, JsonNodeWrapper.class));
			others.removeAll();
		}
		
		for(int i=0; i<this.mapping.size(); i++){
			IJsonNode node = ((IObjectNode)value).get(this.mapping.get(i));
			if(node.isMissing()){
				target.setNull(i);
			} else {
				target.setField(i, new JsonNodeWrapper(node));
			}
			
		}
		 
		for(Entry<String, IJsonNode> entry : ((IObjectNode)value).getEntries()){
			if(!this.mapping.contains(entry.getKey())){
				others.put(entry.getKey(), entry.getValue());
			}
		}
		
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
