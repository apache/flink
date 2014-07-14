# Stratosphere Streaming

_"Big Data looks tiny from Stratosphere."_

This repository implements stream data processing support for [Stratosphere](http://www.stratosphere.eu) For more information please check ot the [Architecture Sketch](https://github.com/stratosphere/stratosphere-streaming/wiki/Architecture-Sketch).

##  Build From Source

This tutorial shows how to build Stratosphere Streaming on your own system. Please open a bug report if you have any troubles!

### Requirements
* Unix-like environment (We use Linux, Mac OS X, Cygwin)
* git
* Maven (at least version 3.0.4)
* Java 6 or 7

### Get the source & Build it

```
git clone https://github.com/stratosphere/stratosphere-streaming.git
cd stratosphere
mvn clean assembly:assembly
```
### Run an example

To run an example counting the frequencies of unique words in the text of Shakespeare's [Hamlet](http://www.gutenberg.org/cache/epub/1787/pg1787.txt)

```
java -cp target/stratosphere-streaming-0.5-SNAPSHOT-jar-with-dependencies.jar eu.stratosphere.streaming.examples.wordcount.WordCountLocal
```

## Support
Don’t hesitate to ask!

[Open an issue](https://github.com/stratosphere/stratosphere-streaming/issues/new) on Github, if you found a bug or need any help.
The main project also has a [mailing list](https://groups.google.com/d/forum/stratosphere-dev) for both users and developers.

## Fork and Contribute

This is an active open-source project. We are always open to people who want to use the system or contribute to it. 
Contact us if you are looking for implementation tasks that fit your skills.

The main project a list of [starter jobs](https://github.com/stratosphere/stratosphere/wiki/Starter-Jobs) in our wiki.

We use the GitHub Pull Request system for the development of Stratosphere. Just open a request if you want to contribute.

### What to contribute
* Bug reports
* Bug fixes
* Documentation
* Tools that ease the use and development of Stratosphere
* Well-written Stratosphere jobs

Let us know if you have created a system that uses Stratosphere, so that we can link to you.
