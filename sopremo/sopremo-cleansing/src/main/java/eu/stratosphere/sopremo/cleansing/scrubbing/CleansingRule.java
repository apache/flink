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

}