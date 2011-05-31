package eu.stratosphere.simple.jaql;

import com.ibm.jaql.lang.expr.core.Expr;

import eu.stratosphere.sopremo.SerializableSopremoType;

interface JaqlToSopremoParser<Result extends SerializableSopremoType> {
	public abstract Result parse(Expr expr);
}