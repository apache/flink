package eu.stratosphere.sopremo;

import java.io.InputStream;

import com.ibm.jaql.lang.expr.core.Expr;

public abstract class PlanCreator {
	public abstract Plan getPlan(InputStream stream);
}
