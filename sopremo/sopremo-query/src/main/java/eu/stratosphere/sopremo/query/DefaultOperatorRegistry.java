package eu.stratosphere.sopremo.query;

import eu.stratosphere.sopremo.operator.Name;
import eu.stratosphere.sopremo.operator.Operator;
import eu.stratosphere.sopremo.packages.DefaultNameChooser;
import eu.stratosphere.sopremo.packages.DefaultRegistry;
import eu.stratosphere.sopremo.packages.NameChooser;
import eu.stratosphere.util.reflect.ReflectUtil;

public class DefaultOperatorRegistry extends DefaultRegistry<OperatorInfo<?>> implements IOperatorRegistry {
	/**
	 * 
	 */
	private static final long serialVersionUID = 5277284740311166569L;

	private final NameChooser operatorNameChooser = new DefaultNameChooser();

	private final NameChooser propertyNameChooser = new DefaultNameChooser();

	@Override
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void put(final Class<? extends Operator<?>> operatorClass) {
		String name = this.getName(operatorClass);

		if (this.get(name) != null)
			throw new IllegalStateException("Duplicate operator " + name);

		this.put(name, new OperatorInfo(operatorClass, name, this.propertyNameChooser));
	}

	@Override
	public String getName(final Class<? extends Operator<?>> operatorClass) {
		String name;
		final Name nameAnnotation = ReflectUtil.getAnnotation(operatorClass, Name.class);
		if (nameAnnotation != null)
			name = this.operatorNameChooser.choose(nameAnnotation.noun(), nameAnnotation.verb(),
				nameAnnotation.adjective(),
				nameAnnotation.preposition());
		else
			name = operatorClass.getSimpleName();
		return name;
	}
}
