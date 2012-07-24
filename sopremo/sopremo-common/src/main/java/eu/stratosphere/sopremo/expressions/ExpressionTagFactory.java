package eu.stratosphere.sopremo.expressions;

import java.util.HashMap;
import java.util.Map;

public class ExpressionTagFactory {
	private final Map<String, ExpressionTag> tags = new HashMap<String, ExpressionTag>();

	public ExpressionTagFactory() {
		this.register(ExpressionTag.RETAIN);
	}

	public void register(final ExpressionTag tag) {
		this.tags.put(tag.toString(), tag);
	}

	public ExpressionTag getTag(final String name) {
		return this.tags.get(name);
	}
}
