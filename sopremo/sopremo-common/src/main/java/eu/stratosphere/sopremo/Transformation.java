package eu.stratosphere.sopremo;

import java.util.ArrayList;
import java.util.List;

import eu.stratosphere.dag.Navigator;
import eu.stratosphere.reflect.TypeHandler;
import eu.stratosphere.reflect.TypeSpecificHandler;

public class Transformation extends Mapping {
	public static final Transformation IDENTITY = new Transformation();

	private List<Mapping> mappings = new ArrayList<Mapping>();

	private boolean array;

	public Transformation() {
		this(null);
	}

	public Transformation(String target) {
		super(target);
	}

	// public Transformation(boolean array) {
	// }

	public void addMapping(Mapping mapping) {
		this.mappings.add(mapping);
	}

	public List<Mapping> getMappings() {
		return mappings;
	}

	public int getMappingSize() {
		return this.mappings.size();
	}

	public Mapping getMapping(int index) {
		return this.mappings.get(index);
	}

	@Override
	public String toString() {
		if (this.getTarget() == null)
			return this.mappings.toString();
		return String.format("%s=%s", this.getTarget(), this.mappings);
	}

	private static TypeSpecificHandler<Mapping, Mapping, TypeHandler<Mapping, Mapping>> PathReplacer = new TypeSpecificHandler<Mapping, Mapping, TypeHandler<Mapping, Mapping>>();

	static {
		PathReplacer.register(ValueAssignment.class, new TypeHandler<ValueAssignment, Mapping>() {
			public ValueAssignment replace(ValueAssignment assignment, List<Mapping> mapping, JsonPath toReplace,
					JsonPath replaceFragment) {
				assignment.setTransformation(JsonPath.replace(assignment.getTransformation(), toReplace, replaceFragment));
				return assignment;
			}
		});
	}

	public void replace(JsonPath toReplace, JsonPath replaceFragment) {
		PathReplacer.handleRecursively(new MappingNavigator(), this, toReplace, replaceFragment);
	}

	private static final class MappingNavigator implements Navigator<Mapping> {
		private static final Iterable<Mapping> EMPTY = new ArrayList<Mapping>();

		@Override
		public Iterable<Mapping> getConnectedNodes(Mapping node) {
			if (node instanceof Transformation)
				return ((Transformation) node).getMappings();
			return EMPTY;
		}
	}
}
