package eu.stratosphere.meteor;

import java.util.IdentityHashMap;
import java.util.Map;

public class TraceableSopremoTreeAdaptor extends SopremoTreeAdaptor {
	private final Map<Object, Object[]> instantiationInfos = new IdentityHashMap<Object, Object[]>();

	public void addJavaFragment(final Object result, final StringBuilder builder) {
		final Object[] params = this.instantiationInfos.get(result);

		builder.append("new ").append(result.getClass().getSimpleName()).append("(");
		if (params == null)
			builder.append(result);
		else
			for (int index = 0; index < params.length; index++) {
				if (index > 0) {
					builder.append(",");
					builder.append("\n");
				}
				this.addJavaFragment(params[index], builder);
			}
		builder.append(")");
	}

	@Override
	protected Object instantiate(final Class<?> expressionClass, final Object[] params) {
		final Object result = super.instantiate(expressionClass, params);
		this.instantiationInfos.put(result, params);
		return result;
	}

}
