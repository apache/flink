package eu.stratosphere.sopremo.cleansing.scrubbing;

import java.util.Arrays;
import java.util.List;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.expressions.SopremoExpression;

public abstract class CleansingRule<ContextType extends EvaluationContext> extends SopremoExpression<ContextType> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1801909303463739160L;

	private final List<String> targetPath;

	public CleansingRule(final String... targetPath) {
		this.targetPath = Arrays.asList(targetPath);
	}

	public List<String> getTargetPath() {
		return this.targetPath;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + this.targetPath.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		CleansingRule<?> other = (CleansingRule<?>) obj;
		return this.targetPath.equals(other.targetPath);
	}

}