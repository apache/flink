# Stratosphere

_"Big Data looks tiny from Stratosphere."_

Stratosphere is a next-generation Big Data Analytics Platform. It combines the strenghts of MapReduce/Hadoop with powerful programming abstractions in Java and Scala, and a high performance runtime. Stratosphere has native support for iterations, incremental iterations, and programs consisting of workflows of many operations.

Learn more about Stratosphere at http://stratosphere.eu

## Start writing a Stratosphere Job
If you just want to get started with Stratosphere, use the following command to set up an empty Stratosphere Job

```
curl https://raw.github.com/stratosphere/stratosphere-quickstart/master/quickstart.sh | bash
```
The quickstart sample contains everything to develop a Stratosphere Job on your computer and run it in a local embedded runtime. No setup needed.
Further quickstart guides are at http://stratosphere.eu/quickstart/


## Build Stratosphere
Below are three short tutorials that guide you through the first steps: Building, running and developing.

###  Build From Source

This tutorial shows how to build Stratosphere on your own system. Please open a bug report if you have any troubles!

#### Requirements
* Unix-like environment (We use Linux, Mac OS X, Cygwin)
* git
* Maven (at least version 3.0.4)
* Java 6 or 7

```
git clone https://github.com/stratosphere/stratosphere.git
cd stratosphere
mvn -DskipTests clean package # this will take up to 5 minutes
```

Stratosphere is now installed in `stratosphere-dist/target`
If you’re a Debian/Ubuntu user, you’ll find a .deb package. We will continue with the generic case.

	cd stratosphere-dist/target/stratosphere-dist-0.4-SNAPSHOT-bin/stratosphere-0.4-SNAPSHOT/

The directory structure here looks like the contents of the official release distribution.

#### Build for different Hadoop Versions
This section is for advanced users that want to build Stratosphere for a different Hadoop version, for example for Hadoop Yarn support.

We use the profile activation via properties (-D).

##### Build hadoop v1 (default)
Build the default (currently hadoop 1.2.1)
```mvn clean package```

Build for a specific hadoop v1 version
```mvn -Dhadoop-one.version=1.1.2 clean package```

##### Build hadoop v2 (yarn)

Build the yarn using the default version defined in the pom
```mvn -Dhadoop.profile=2 clean package```

Build for a specific hadoop v1 version
```mvn -Dhadoop.profile=2 -Dhadoop-two.version=2.1.0-beta clean package```

It is necessary to generate separate POMs if you want to deploy to your local repository (`mvn install`) or somewhere else.
We have a script in `/tools` that generates POMs for the profiles. Use 
```mvn -f pom.hadoop2.xml clean install -DskipTests```
to put a POM file with the right dependencies into your local repository.


### Run your first program

We will run a simple “Word Count” example. 
The easiest way to start Stratosphere on your local machine is so-called "local-mode":

	./bin/start-local.sh

Get some test data:

	 wget -O hamlet.txt http://www.gutenberg.org/cache/epub/1787/pg1787.txt

Start the job:

	./bin/pact-client.sh run --jarfile ./examples/pact/pact-examples-0.4-SNAPSHOT-WordCount.jar --arguments 1 file://`pwd`/hamlet.txt file://`pwd`/wordcount-result.txt

You will find a file called `wordcount-result.txt` in your current directory.

#### Alternative Method: Use the PACT web interface
(And get a nice execution plan overview for free!)

	./bin/start-local.sh
	./bin/pact-webfrontend.sh start

Get some test data:
	 wget -O ~/hamlet.txt http://www.gutenberg.org/cache/epub/1787/pg1787.txt

* Point your browser to to http://localhost:8080/launch.html. Upload the WordCount.jar using the upload form in the lower right box. The jar is located in `./examples/pact/pact-examples-0.4-WordCount.jar`.
* Select the WordCount jar from the list of available jars (upper left).
* Enter the argument line in the lower-left box: `1 file://<path to>/hamlet.txt file://<wherever you want the>/wordcount-result.txt`

* Hit “Run Job”


### Eclipse Setup and Debugging

To contribute back to the project or develop your own jobs for Stratosphere, you need a working development environment. We use Eclipse and IntelliJ for development. Here we focus on Eclipse.

If you want to work on the scala code you will need the following plugins:

Eclipse 4.x:
  * scala-ide: http://download.scala-ide.org/sdk/e38/scala210/stable/site
  * m2eclipse-scala: http://alchim31.free.fr/m2e-scala/update-site
  * build-helper-maven-plugin: https://repository.sonatype.org/content/repositories/forge-sites/m2e-extras/0.15.0/N/0.15.0.201206251206/

Eclipse 3.7:
  * scala-ide: http://download.scala-ide.org/sdk/e37/scala210/stable/site
  * m2eclipse-scala: http://alchim31.free.fr/m2e-scala/update-site
  * build-helper-maven-plugin: https://repository.sonatype.org/content/repositories/forge-sites/m2e-extras/0.14.0/N/0.14.0.201109282148/

When you don't have the plugins your project will have build errors, you can just close the scala projects and ignore them.
o
Import the Stratosphere source code using Maven's Import tool:
  * Select "Import" from the "File"-menu.
  * Expand "Maven" node, select "Existing Maven Projects", and click "next" button
  * Select the root directory by clicking on the "Browse" button and navigate to the top folder of the cloned Stratosphere Git repository.
  * Ensure that all projects are selected and click the "Finish" button.

Create a new Eclipse Project that requires Stratosphere in its Build Path!

Use this skeleton as an entry point for your own Jobs: It allows you to hit the “Run as” -> “Java Application” feature of Eclipse. (You have to stop the application manually, because only one instance can run at a time)

```java
public class Tutorial implements PlanAssembler, PlanAssemblerDescription {

	public static void execute(Plan toExecute) throws Exception {
		LocalExecutor executor = new LocalExecutor();
		executor.start();
		long runtime = executor.executePlan(toExecute);
		System.out.println("runtime:  " + runtime);
		executor.stop();
	}

	@Override
	public Plan getPlan(String... args) {
		// your Plan goes here
	}

	@Override
	public String getDescription() {
		return "Usage: …. "; // TODO
	}

	public static void main(String[] args) throws Exception {
		Tutorial tut = new Tutorial();
		Plan toExecute = tut.getPlan( /* Arguments */);
		execute(toExecute);
	}
}

```

## Support
Don’t hesitate to ask!

[Open an issue](https://github.com/stratosphere/stratosphere/issues/new) on Github, if you found a bug or need any help.
We also have a [mailing list](https://groups.google.com/d/forum/stratosphere-dev) for both users and developers.

Some of our colleagues are also in the #dima irc channel on freenode.

## Documentation

There is our (old) [official Wiki](https://stratosphere.eu/wiki/doku).
We are in the progress of migrating it to the [GitHub Wiki](https://github.com/stratosphere/stratosphere/wiki/_pages)

Please make edits to the Wiki if you find inconsistencies or [Open an issue](https://github.com/stratosphere/stratosphere/issues/new) 


## Fork and Contribute

This is an active open-source project. We are always open to people who want to use the system or contribute to it. 
Contact us if you are looking for implementation tasks that fit your skills.

We use the GitHub Pull Request system for the development of Stratosphere. Just open a request if you want to contribute.

### What to contribute
* Bug reports
* Bug fixes
* Documentation
* Tools that ease the use and development of Stratosphere
* Well-written Stratosphere jobs


Let us know if you have created a system that uses Stratosphere, so that we can link to you.

## About

[Stratosphere](http://www.stratosphere.eu) is a DFG-founded research project. Ozone is the codename of the latest Stratosphere distribution. 
We combine cutting edge research outcomes with a stable and usable codebase.
Decisions are not made behind closed doors. We discuss all changes and plans on our Mailinglists and on GitHub.


Build Status: [![Build Status](https://travis-ci.org/stratosphere/stratosphere.png)](https://travis-ci.org/stratosphere/stratosphere)





