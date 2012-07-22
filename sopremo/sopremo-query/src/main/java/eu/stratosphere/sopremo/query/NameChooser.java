package eu.stratosphere.sopremo.query;

public interface NameChooser {
	public String choose(String[] nouns, String[] verbs, String[] adjectives, String[] prepositions);
}