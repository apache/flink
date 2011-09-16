package eu.stratosphere.sopremo;

import java.util.HashMap;
import java.util.Map;

public class ExpressionTagFactory {
	private Map<String, ExpressionTag> tags = new HashMap<String, ExpressionTag>();
	
	public ExpressionTagFactory() {
		register(ExpressionTag.RETAIN);
	}
	
	public void register(ExpressionTag tag) {
		tags.put(tag.toString(), tag);
	}
	
	public ExpressionTag getTag(String name) {
		return tags.get(name);
	}
}
