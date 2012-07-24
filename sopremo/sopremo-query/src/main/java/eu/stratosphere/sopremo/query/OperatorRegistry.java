package eu.stratosphere.sopremo.query;

import java.util.HashMap;
import java.util.Map;

import eu.stratosphere.sopremo.io.Sink;
import eu.stratosphere.sopremo.io.Source;
import eu.stratosphere.sopremo.operator.Name;
import eu.stratosphere.sopremo.operator.Operator;
import eu.stratosphere.util.reflect.ReflectUtil;

public class OperatorRegistry {

	private final NameChooser operatorNameChooser = new DefaultNameChooser();

	private final NameChooser propertyNameChooser = new DefaultNameChooser();

	private Map<String, OperatorInfo<?>> operators = new HashMap<String, OperatorInfo<?>>();

	public OperatorRegistry() {
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
			name = operatorClass.getSimpleName();

		if (this.operators.containsKey(name))
			throw new IllegalStateException("Duplicate operator " + name);

		this.operators.put(name, new OperatorInfo(operatorClass, name, this.propertyNameChooser));
	}

	@SuppressWarnings("unchecked")
	public <O extends Operator<O>> OperatorInfo<O> getOperatorInfo(final Class<O> operatorClass) {
		for (final OperatorInfo<?> info : this.operators.values())
			if (info.operatorClass == operatorClass)
				return (OperatorInfo<O>) info;
		return null;
	}

	public OperatorInfo<?> getOperatorInfo(final String name) {
		return this.operators.get(name);
	}

	public Map<String, OperatorInfo<?>> getOperatorInfos() {
		return this.operators;
	}
}
