package eu.stratosphere.simple;

import java.io.InputStream;

import eu.stratosphere.sopremo.SopremoPlan;

public abstract class PlanCreator {
	public abstract SopremoPlan getPlan(InputStream stream);
}
