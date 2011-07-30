package eu.stratosphere.sopremo.cleansing.scrubbing;

import eu.stratosphere.sopremo.cleansing.conflict_resolution.ConflictResolution;
import eu.stratosphere.sopremo.expressions.BooleanExpression;

public abstract class ConstraintExpression extends BooleanExpression {
	public abstract ConflictResolution getDefaultResolution();
}
