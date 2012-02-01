package eu.stratosphere.sopremo.type;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.sopremo.pact.JsonNodeWrapper;
import eu.stratosphere.sopremo.pact.SopremoUtil;

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
	public PactRecord jsonToRecord(JsonNode value, PactRecord target) {
		if(target == null){
			
			//the last element is the field "others"
			target = new PactRecord(this.mapping.size() + 1);
		}
		
		for(int i=0; i<this.mapping.size(); i++){
			target.setField(i, new JsonNodeWrapper(((ObjectNode)value).get(this.mapping.get(i))));
			((ObjectNode)value).remove(this.mapping.get(i));
		}
		
		target.setField(this.mapping.size(), new JsonNodeWrapper(value));
		
		return target;
	}

	@Override
	public JsonNode recordToJson(PactRecord record, JsonNode target) {
		if(target == null){
			target = new ObjectNode();
		} else {
			((ObjectNode)target).removeAll();
		}
		
		for(int i=0; i< this.mapping.size(); i++){
			((ObjectNode)target).put(this.mapping.get(i), SopremoUtil.unwrap(record.getField(i, JsonNodeWrapper.class)));
		}
		((ObjectNode)target).putAll((ObjectNode)SopremoUtil.unwrap(record.getField(this.mapping.size(), JsonNodeWrapper.class)));
		
		return target;
	}
	
}
