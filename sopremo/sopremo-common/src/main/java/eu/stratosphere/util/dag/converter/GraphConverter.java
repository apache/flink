package eu.stratosphere.util.dag.converter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import eu.stratosphere.util.OneElementList;
import eu.stratosphere.util.dag.Navigator;
import eu.stratosphere.util.reflect.BoundTypeUtil;

/**
 * Converts a directly acyclic graph to another by performing recursive invocation of {@link NodeConverter}s for
 * specific node types.
 * 
 * @author Arvid Heise
 * @param <InputType>
 *        the type of the input node
 * @param <OutputType>
 *        the type of the output node
 */
public class GraphConverter<InputType, OutputType> implements NodeConverter<InputType, OutputType> {

	private Map<Class<?>, NodeConverterInfo<InputType, OutputType>> converterInfos = new HashMap<Class<?>, NodeConverterInfo<InputType, OutputType>>();

	private List<GraphConversionListener<InputType, OutputType>> conversionListener = new ArrayList<GraphConversionListener<InputType, OutputType>>();

	private boolean flattenCollection = true;

	@SuppressWarnings("unchecked")
	private List<OutputType> lastChildren = Collections.EMPTY_LIST;

	/**
	 * Adds a {@link GraphConversionListener} that is invoked before and after a specific node is converted.
	 * 
	 * @param listener
	 *        the listener to add
	 */
	public void addListener(GraphConversionListener<InputType, OutputType> listener) {
		this.conversionListener.add(listener);
	}

	@SuppressWarnings("unchecked")
	private List<OutputType> convertChildren(Navigator<InputType> navigator, InputType root,
			NodeConverterInfo<InputType, OutputType> converterInfo) {
		List<OutputType> childTypes = new ArrayList<OutputType>();

		if (converterInfo == null || !converterInfo.isStopRecursion())
			for (InputType child : navigator.getConnectedNodes(root)) {
				OutputType handledResult = this.convertGraph(child, navigator);
				if (this.flattenCollection && handledResult instanceof Collection<?>)
					childTypes.addAll((Collection<? extends OutputType>) handledResult);
				else if (handledResult != null)
					childTypes.add(handledResult);
			}
		childTypes.addAll(this.lastChildren);
		return childTypes;
	}

	/**
	 * Converts a graph given by the start node and the referenced nodes reachable with the {@link Navigator}.<br>
	 * For each node the registered {@link NodeConverter} is applied recursively until every reachable node has been
	 * converted.<br>
	 * If a node without appropriate {@link NodeConverter} appears or the converter return null, the first converted
	 * child is returned instead.
	 * 
	 * @param startNode
	 *        the start node
	 * @param navigator
	 *        the navigator
	 * @return the converted start node
	 */
	public OutputType convertGraph(InputType startNode, Navigator<InputType> navigator) {
		return this.convertGraph(navigator, startNode, new IdentityHashMap<InputType, OutputType>());
	}

	/**
	 * Converts a graph given by the start nodes and the referenced nodes reachable with the {@link Navigator}.<br>
	 * For each node the registered {@link NodeConverter} is applied recursively until every reachable node has been
	 * converted.<br>
	 * If a node without appropriate {@link NodeConverter} appears or the converter return null, the first converted
	 * child is returned instead.
	 * 
	 * @param startNodes
	 *        the start nodes
	 * @param navigator
	 *        the navigator
	 * @return the converted start nodes
	 */
	public List<OutputType> convertGraph(InputType[] startNodes, Navigator<InputType> navigator) {
		return this.convertGraph(Arrays.asList(startNodes).iterator(), navigator);
	}

	/**
	 * Converts a graph given by the start nodes and the referenced nodes reachable with the {@link Navigator}.<br>
	 * For each node the registered {@link NodeConverter} is applied recursively until every reachable node has been
	 * converted.<br>
	 * If a node without appropriate {@link NodeConverter} appears or the converter return null, the first converted
	 * child is returned instead.
	 * 
	 * @param startNodes
	 *        the start nodes
	 * @param navigator
	 *        the navigator
	 * @return the converted start nodes
	 */
	public List<OutputType> convertGraph(Iterable<InputType> startNodes, Navigator<InputType> navigator) {
		return this.convertGraph(startNodes.iterator(), navigator);
	}

	/**
	 * Converts a graph given by the start nodes and the referenced nodes reachable with the {@link Navigator}.<br>
	 * For each node the registered {@link NodeConverter} is applied recursively until every reachable node has been
	 * converted.<br>
	 * If a node without appropriate {@link NodeConverter} appears or the converter return null, the first converted
	 * child is returned instead.
	 * 
	 * @param startNodes
	 *        the start nodes
	 * @param navigator
	 *        the navigator
	 * @return the converted start nodes
	 */
	public List<OutputType> convertGraph(Iterator<InputType> startNodes, Navigator<InputType> navigator) {
		List<OutputType> results = new ArrayList<OutputType>();
		IdentityHashMap<InputType, OutputType> convertedNodes = new IdentityHashMap<InputType, OutputType>();
		while (startNodes.hasNext())
			results.add(this.convertGraph(navigator, startNodes.next(), convertedNodes));
		return results;
	}

	@SuppressWarnings("unchecked")
	private OutputType convertGraph(Navigator<InputType> navigator, InputType root,
			IdentityHashMap<InputType, OutputType> convertedNodes) {
		if (convertedNodes.containsKey(root))
			return convertedNodes.get(root);

		NodeConverterInfo<InputType, OutputType> converterInfo = this.getNodeConverterInfo(root.getClass());
		for (GraphConversionListener<InputType, OutputType> listener : this.conversionListener)
			listener.beforeSubgraphConversion(root);
		List<OutputType> childTypes = this.convertChildren(navigator, root, converterInfo);

		this.lastChildren = converterInfo != null && converterInfo.shouldAppendChildren() ? childTypes.subList(
			converterInfo.getAppendIndex(), childTypes.size()) : Collections.EMPTY_LIST;
		OutputType convertedType = this.convertNode(root, childTypes);
		for (GraphConversionListener<InputType, OutputType> listener : this.conversionListener)
			listener.afterSubgraphConversion(root, convertedType);
		if (convertedType == null)
			return childTypes.isEmpty() ? null : childTypes.get(0);
		return convertedType;
	}

	@Override
	public OutputType convertNode(InputType in, List<OutputType> children) {
		NodeConverterInfo<InputType, OutputType> converterInfo = this.getNodeConverterInfo(in.getClass());
		if (converterInfo == null)
			return null;
		try {
			for (GraphConversionListener<InputType, OutputType> listener : this.conversionListener)
				listener.beforeNodeConversion(in, children);
			OutputType converted = converterInfo.getConverter().convertNode(in, children);
			for (GraphConversionListener<InputType, OutputType> listener : this.conversionListener)
				listener.afterNodeConversion(in, children, converted);
			return converted;
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}

	/**
	 * Returns the registered converter for the given node class or base classes.
	 * 
	 * @param clazz
	 *        the node class
	 * @return the registered converter for the given node class
	 */
	public NodeConverter<InputType, OutputType> getNodeConverter(Class<? extends InputType> clazz) {
		NodeConverterInfo<InputType, OutputType> converterInfo = this.getNodeConverterInfo(clazz);
		return converterInfo == null ? null : converterInfo.converter;
	}

	private NodeConverterInfo<InputType, OutputType> getNodeConverterInfo(Class<?> clazz) {
		NodeConverterInfo<InputType, OutputType> converterInfo = this.converterInfos.get(clazz);
		if (converterInfo == null && clazz.getSuperclass() != null) {
			converterInfo = this.getNodeConverterInfo(clazz.getSuperclass());
			if (converterInfo != null)
				this.converterInfos.put(clazz, converterInfo);
		}
		return converterInfo;
	}

	/**
	 * Registers the {@link NodeConverter} for the specified types.
	 * 
	 * @param <BaseInputType>
	 *        the base type for all types
	 * @param converter
	 *        the converter to register
	 * @param types
	 *        a list of all types for which
	 * @return this
	 */
	public <BaseInputType extends InputType> GraphConverter<InputType, OutputType> register(
			NodeConverter<? extends BaseInputType, ? extends OutputType> converter,
			List<Class<? extends BaseInputType>> types) {
		NodeConverterInfo<InputType, OutputType> converterInfo = new NodeConverterInfo<InputType, OutputType>(
			converter);
		for (Class<? extends InputType> type : types)
			this.converterInfos.put(type, converterInfo);
		return this;
	}

	/**
	 * Registers the {@link NodeConverter} for the given BaseInputType inferred from the static bound of the generic
	 * class parameter in the NodeConverter.
	 * 
	 * @param converter
	 *        the converter to register
	 * @param <BaseInputType>
	 *        the base type
	 * @return this
	 */
	@SuppressWarnings("unchecked")
	public <BaseInputType extends InputType> GraphConverter<InputType, OutputType> register(
			NodeConverter<BaseInputType, ? extends OutputType> converter) {
		OneElementList<Class<? extends BaseInputType>> wrapList = new OneElementList<Class<? extends BaseInputType>>(
				(Class<? extends BaseInputType>) BoundTypeUtil
						.getBindingOfSuperclass(converter.getClass(),
							NodeConverter.class).getParameters()[0].getType());
		this.register(converter, wrapList);
		return this;
	}

	/**
	 * Registers the {@link NodeConverter}s for the types inferred from the static bound of the generic
	 * class parameter in the NodeConverter.
	 * 
	 * @param converters
	 *        the converters to register
	 * @return this
	 */
	public GraphConverter<InputType, OutputType> registerAll(
			NodeConverter<? extends InputType, ? extends OutputType>... converters) {
		for (NodeConverter<? extends InputType, ? extends OutputType> converter : converters)
			this.register(converter);
		return this;
	}

	/**
	 * Removes a {@link GraphConversionListener} from this GraphConverter.
	 * 
	 * @param listener
	 *        the listener to remove
	 */
	public void removeListener(GraphConversionListener<InputType, OutputType> listener) {
		this.conversionListener.remove(listener);
	}

	/**
	 * Removes the {@link NodeConverter} for the given type.
	 * 
	 * @param type
	 *        the type of which the associated NodeConverter is removed
	 */
	public void unregister(Class<? extends InputType> type) {
		this.converterInfos.remove(type);
	}

	/**
	 * Removes the given {@link NodeConverter}.
	 * 
	 * @param converter
	 *        the converter to remove
	 */
	public void unregister(NodeConverter<InputType, OutputType> converter) {
		Iterator<? extends Entry<?, ?>> iterator = this.converterInfos.entrySet().iterator();
		for (; iterator.hasNext();)
			if (iterator.next().getValue() == converter)
				iterator.remove();
	}

	/**
	 * Holds additional information about a converter mostly extracted from annotations.
	 * 
	 * @author Arvid Heise
	 */
	private static class NodeConverterInfo<InputType, OutputBase> {
		private int appendIndex = -1;

		private boolean stopRecursion = false;

		private NodeConverter<InputType, OutputBase> converter;

		@SuppressWarnings("unchecked")
		public NodeConverterInfo(NodeConverter<? extends InputType, ? extends OutputBase> converter) {
			this.converter = (NodeConverter<InputType, OutputBase>) converter;

			Class<?> converterClass = converter.getClass();
			AppendChildren annotation = converterClass.getAnnotation(AppendChildren.class);
			if (annotation != null)
				this.appendIndex = annotation.fromIndex();
			this.stopRecursion = converterClass.getAnnotation(Leaf.class) != null;
		}

		public int getAppendIndex() {
			return this.appendIndex;
		}

		public NodeConverter<InputType, OutputBase> getConverter() {
			return this.converter;
		}

		public boolean isStopRecursion() {
			return this.stopRecursion;
		}

		public boolean shouldAppendChildren() {
			return this.appendIndex != -1;
		}
	}
}