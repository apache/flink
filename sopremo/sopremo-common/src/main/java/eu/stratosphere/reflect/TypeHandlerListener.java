package eu.stratosphere.reflect;

import java.util.List;

public interface TypeHandlerListener<InputType, OutputType> {
	public void beforeConversion(InputType in, List<OutputType> children);

	public void afterConversion(InputType in, List<OutputType> children, OutputType out);

	public void beforeHierarchicalConversion(InputType in);

	public void afterHierarchicalConversion(InputType in, OutputType out);
}
