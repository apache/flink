package eu.stratosphere.sopremo;

import java.io.InputStream;


public abstract class PlanCreator {
	public abstract SopremoPlan getPlan(InputStream stream);
}
