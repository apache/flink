package eu.stratosphere.sopremo.packages;

import java.io.Serializable;

public interface NameChooser extends Serializable {
	public String choose(String[] nouns, String[] verbs, String[] adjectives, String[] prepositions);
}