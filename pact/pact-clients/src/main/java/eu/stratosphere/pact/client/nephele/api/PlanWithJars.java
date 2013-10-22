package eu.stratosphere.pact.client.nephele.api;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import eu.stratosphere.pact.common.plan.Plan;

public class PlanWithJars {
	private Plan plan;
	private List<String> jarFiles;

	public PlanWithJars(Plan plan, List<String> jarFiles) throws IOException {
		this.plan = plan;
		this.jarFiles = jarFiles;
		for (String jar: jarFiles) {
			File file = new File(jar);
			checkJarFile(file);
		}
	}
	
	public PlanWithJars(Plan plan, String jarFile) throws IOException {
		this(plan, new LinkedList<String>());
		checkJarFile(new File(jarFile));
		jarFiles.add(jarFile);
	}

	/**
	 * Returns the plan
	 */
	public Plan getPlan() {
		return this.plan;
	}

	/**
	 * Returns list of jar files that need to be submitted with the plan.
	 */
	public List<File> getJarFiles() throws IOException {
		List<File> result = new ArrayList<File>(jarFiles.size());
		for (String jar: jarFiles) {
			result.add(new File(jar));
		}
		return result;
	}
	

	public static void checkJarFile(File jar) throws IOException {
		if (!jar.exists()) {
			throw new IOException("JAR file does not exist '" + jar.getAbsolutePath() + "'");
		}
		if (!jar.canRead()) {
			throw new IOException("JAR file can't be read '" + jar.getAbsolutePath() + "'");
		}
		// TODO: Check if proper JAR file
	}
}