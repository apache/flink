package eu.stratosphere.sopremo.query;

import eu.stratosphere.sopremo.operator.Name;
import eu.stratosphere.sopremo.operator.Operator;
import eu.stratosphere.sopremo.packages.DefaultRegistry;
import eu.stratosphere.util.reflect.ReflectUtil;

public class DefaultOperatorRegistry extends DefaultRegistry<OperatorInfo<?>> implements IOperatorRegistry {
	/**
	 * 
	 */
	private static final long serialVersionUID = 5277284740311166569L;
	

	private final NameChooser operatorNameChooser = new DefaultNameChooser();

	private final NameChooser propertyNameChooser = new DefaultNameChooser();
	


	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void put(final Class<? extends Operator<?>> operatorClass) {
		String name;
		final Name nameAnnotation = ReflectUtil.getAnnotation(operatorClass, Name.class);
		if (nameAnnotation != null)
			name = this.operatorNameChooser.choose(nameAnnotation.noun(), nameAnnotation.verb(),
				nameAnnotation.adjective(),
				nameAnnotation.preposition());
		else
			name = operatorClass.getSimpleName();

		if (this.get(name) != null)
			throw new IllegalStateException("Duplicate operator " + name);

		put(name, new OperatorInfo(operatorClass, name, this.propertyNameChooser));
	}

//	@SuppressWarnings("unchecked")
//	public <O extends Operator<O>> OperatorInfo<O> get(final Class<O> operatorClass) {
//		for (final OperatorInfo<?> info : this.operators.values())
//			if (info.operatorClass == operatorClass)
//				return (OperatorInfo<O>) info;
//		return null;
//	}
}
