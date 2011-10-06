package eu.stratosphere.simple.jaql;

import java.util.IdentityHashMap;
import java.util.Map;

public class TraceableSopremoTreeAdaptor extends SopremoTreeAdaptor {
	private Map<Object, Object[]> instantiationInfos = new IdentityHashMap<Object, Object[]>();

	public void addJavaFragment(Object result, StringBuilder builder) {
		Object[] params = this.instantiationInfos.get(result);

		builder.append("new ").append(result.getClass()).append("(");
		for (int index = 0; index < params.length; index++) {
			if (index > 0)
				builder.append(",");
			builder.append("\n");
			this.addJavaFragment(params[index], builder);
		}
		builder.append(")");
	}

	@Override
	protected Object instantiate(Class<?> expressionClass, Object[] params) {
		Object result = super.instantiate(expressionClass, params);
		this.instantiationInfos.put(result, params);
		return result;
	}

}
