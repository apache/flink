# ozone


This is the source code repository of the Stratosphere research project. 

See www.stratosphere.eu for project details, publications, etc

ozone is the codename of the latest Stratosphere distribution.



Build Status: [![Build Status](https://travis-ci.org/dimalabs/ozone.png)](https://travis-ci.org/dimalabs/ozone)

## Getting Started
Below are three short tutorials that guide you through the first steps: Building, running and developing.

###  Build From Source

This tutorial shows how to build Stratosphere on your own system. Please open a bug report if you have any troubles!

#### Requirements
* Unix-like environment (We use Linux, Mac OS X, it should run with cygwin)
* git
* maven
* Java 6 or 7

.

	git clone https://github.com/dimalabs/ozone.git
	cd ozone
	mvn -DskipTests clean package # this will take up to 5 minutes

Stratosphere is now installed in `stratosphere-dist/target`
If you’re a Debian/Ubuntu user, you’ll find a .deb package. We will continue with the generic case.

	cd stratosphere-dist/target/stratosphere-dist-0.2-ozone-bin/stratosphere-0.2-ozone/

Note: The directory structure here looks like the contents of the official release distribution.

### Run your first program

We will run a simple “Word Count” example. 
The easiest way to start Stratosphere on your local machine is so-called "local-mode":

	./bin/start-local.sh

Get some test data:

	 wget -O hamlet.txt http://www.gutenberg.org/cache/epub/1787/pg1787.txt

Start the job:

	./bin/pact-client.sh run --jarfile ./examples/pact/pact-examples-0.2-ozone-WordCount.jar --arguments 1 file://`pwd`/hamlet.txt file://`pwd`/wordcount-result.txt

You will find a file called `wordcount-result.txt` in your current directory.

#### Alternative Method: Use the PACT web interface
(And get a nice execution plan overview for free!)

	./bin/start-local.sh
	./bin/pact-webfrontend.sh start

Get some test data:
	 wget -O ~/hamlet.txt http://www.gutenberg.org/cache/epub/1787/pg1787.txt

* Point your browser to to http://localhost:8080/launch.html. Upload the WordCount.jar using the upload form in the lower right box. The jar is located in `./examples/pact/pact-examples-0.2-ozone-WordCount.jar`.
* Select the WordCount jar from the list of available jars (upper left).
* Enter the argument line in the lower-left box: `1 file://<path to>/hamlet.txt file://<wherever you want the>/wordcount-result.txt`

* Hit “Run Job”


### Eclipse Setup and Debugging

To contribute back to the project or develop your own jobs for Stratosphere, you need a working development environment. We use Eclipse and IntelliJ for development. Here we focus on Eclipse.

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

[Open an issue](https://github.com/dimalabs/ozone/issues/new) on github, if you need our help.

We have a mailing list for our users: https://lists.tu-berlin.de/mailman/listinfo/stratosphere-users


## Documentation

There is our (old) [official Wiki](https://stratosphere.eu/wiki/doku).

We are in the progress of migrating it to the [GitHub Wiki](https://github.com/dimalabs/ozone/wiki/_pages)

Please make edits to the Wiki if you find inconsistencies or [Open an issue](https://github.com/dimalabs/ozone/issues/new) 


## Fork and Contribute

This is an active open-source project. We are always open to people who want to use the system or contribute to it. 

The development community lives on GitHub and our [mailing list](https://lists.tu-berlin.de/mailman/listinfo/stratosphere-dev (But we prefer github)

We use the github Pull Request system for the development of Stratosphere. Just open a request if you want to contribute.

### What to contribute
* Bug reports
* Bug fixes
* Documentation
* Tools that ease the use and development of Stratosphere
* Well-written Stratosphere jobs


Let us know if you have created a system that uses Stratosphere, so that we can link to you.







