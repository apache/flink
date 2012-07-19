package eu.stratosphere.sopremo.query;

import java.io.InputStream;

import eu.stratosphere.sopremo.operator.SopremoPlan;

public abstract class PlanCreator {
	public abstract SopremoPlan getPlan(InputStream stream);
}
