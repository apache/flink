package eu.stratosphere.sopremo;

import java.util.HashMap;
import java.util.Map;

import eu.stratosphere.util.reflect.ReflectUtil;

public class OperatorFactory {
	private final Map<String, OperatorInfo<?>> operators = new HashMap<String, OperatorInfo<?>>();

	private final NameChooser operatorNameChooser = new DefaultNameChooser();

	private final NameChooser propertyNameChooser = new DefaultNameChooser();

	public OperatorFactory() {
		this.addOperator(Sink.class);
		this.addOperator(Source.class);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void addOperator(final Class<? extends Operator<?>> operatorClass) {
		String name;
		final Name nameAnnotation = ReflectUtil.getAnnotation(operatorClass, Name.class);
		if (nameAnnotation != null)
			name = this.operatorNameChooser.choose(nameAnnotation.noun(), nameAnnotation.verb(),
				nameAnnotation.adjective(),
				nameAnnotation.preposition());
		else
			name = operatorClass.getSimpleName().toLowerCase();
		this.operators.put(name, new OperatorInfo(operatorClass, name, this.propertyNameChooser));
	}

	@SuppressWarnings("unchecked")
	public <O extends Operator<O>> OperatorInfo<O> getOperatorInfo(final Class<O> operatorClass) {
		for (final OperatorInfo<?> info : this.operators.values())
			if (info.operatorClass == operatorClass)
				return (OperatorInfo<O>) info;
		return null;
	}

	public OperatorInfo<?> getOperatorInfo(final String operator) {
		return this.operators.get(operator.toLowerCase());
	}

	public Map<String, OperatorInfo<?>> getOperatorInfos() {
		return this.operators;
	}

	public static class DefaultNameChooser implements NameChooser {
		private final int[] preferredOrder;

		public DefaultNameChooser() {
			this(3, 0, 1, 2);
		}

		public DefaultNameChooser(final int... preferredOrder) {
			this.preferredOrder = preferredOrder;
		}

		@Override
		public String choose(final String[] nouns, final String[] verbs, final String[] adjectives,
				final String[] prepositions) {
			final String[][] names = { nouns, verbs, adjectives, prepositions };
			for (final int pos : this.preferredOrder) {
				final String value = this.firstOrNull(names[pos]);
				if (value != null)
					return value;
			}
			return null;
		}

		private String firstOrNull(final String[] names) {
			return names == null || names.length == 0 ? null : names[0];
		}
	}

	public static interface NameChooser {
		public String choose(String[] nouns, String[] verbs, String[] adjectives, String[] prepositions);
	}
}
