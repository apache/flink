package eu.stratosphere.sopremo.expressions;

import java.util.Iterator;
import java.util.Map.Entry;


public abstract class ContainerExpression extends EvaluableExpression {
	
	public abstract void replace(EvaluableExpression toReplace, EvaluableExpression replaceFragment);
}
