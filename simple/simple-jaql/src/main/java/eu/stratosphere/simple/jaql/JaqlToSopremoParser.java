package eu.stratosphere.simple.jaql;

import com.ibm.jaql.lang.expr.core.Expr;

import eu.stratosphere.sopremo.SopremoType;

interface JaqlToSopremoParser<Result extends SopremoType> {
	public abstract Result parse(Expr expr);
}