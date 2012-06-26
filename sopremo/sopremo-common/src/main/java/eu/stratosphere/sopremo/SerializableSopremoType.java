package eu.stratosphere.sopremo;

import java.io.Serializable;

/**
 * Tag interface for all serializable Sopremo types.<br>
 * The serialization strategy may be adjusted in the future through the change of this interface.
 * 
 * @author Arvid Heise
 */
public interface SerializableSopremoType extends Serializable, SopremoType {
}
