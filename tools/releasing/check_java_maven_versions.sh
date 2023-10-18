#!/usr/bin/env bash

# Define the required versions
required_java_version="1.8"
required_maven_version="3.8.6"

# Check Java version
java_version=$(java -version 2>&1 | awk -F '"' '/version/ {print $2}')
if [[ "$java_version" == "$required_java_version"* ]]; then
    echo "Java version is correct: $java_version"
else
    echo "Java version is incorrect. Required version: $required_java_version, but it is $java_version"
    exit 1
fi

# Check Maven version
maven_version=$(mvn -v | grep -oE 'Apache Maven [0-9]+\.[0-9]+\.[0-9]+' | awk '{print $3}')
if [[ "$maven_version" == "$required_maven_version" ]]; then
    echo "Maven version is correct: $maven_version"
else
    echo "Maven version is incorrect. Required version: $required_maven_version, but it is $maven_version"
    exit 1
fi
