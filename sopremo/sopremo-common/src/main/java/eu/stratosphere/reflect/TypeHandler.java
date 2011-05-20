package eu.stratosphere.reflect;

import java.util.List;

public interface TypeHandler<InputType, OutputType> {
	 public OutputType convert(InputType in, List<OutputType> children);
}