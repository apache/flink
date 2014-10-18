flink-quickstart
=======================

Two simple quickstart maven archetypes for Flink.

When you import the scala project into eclipse you will also need the following plugins:

Eclipse 4.x:
  * scala-ide: http://download.scala-ide.org/sdk/e38/scala210/stable/site
  * m2eclipse-scala: http://alchim31.free.fr/m2e-scala/update-site
  * build-helper-maven-plugin: https://repository.sonatype.org/content/repositories/forge-sites/m2e-extras/0.15.0/N/0.15.0.201206251206/

Eclipse 3.7:
  * scala-ide: http://download.scala-ide.org/sdk/e37/scala210/stable/site
  * m2eclipse-scala: http://alchim31.free.fr/m2e-scala/update-site
  * build-helper-maven-plugin: https://repository.sonatype.org/content/repositories/forge-sites/m2e-extras/0.14.0/N/0.14.0.201109282148/


# Repository Organization

The quickstart bash scripts do not necessarily point to the most recent version in the code. Since the archetypes are versioned, the quickstarts usually differ by pointing to a specific version.

The `quickstart.sh` script always points to the current stable release (v0.4, v0.5)
`-SNAPSHOT` points to the current snapshot version.


(Use `-DarchetypeCatalog=local` for local testing during archetype development)

# Java 8 with Lambda Expressions

If you are planning to use Java 8 and want to use Lambda Expression, please open the generated "pom.xml" file and modify/uncomment the mentioned lines.
