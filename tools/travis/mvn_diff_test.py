#!/usr/bin/env python
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

from collections import defaultdict
import re, sys

from stages import get_mvn_modules_for_test, STAGE_MISC

def expand_node(graph, node):
    visited = set([node])
    all_dependencies = set()
    queue = list(graph[node])

    while queue:
        n = queue.pop()
        all_dependencies.add(n)
        if n in visited:
            continue
        visited.add(n)
        targets = graph.get(n)
        if targets:
            queue.extend(targets)
    return all_dependencies

def expand_graph(graph):
    new_graph = {}
    for node in graph.keys():
        new_graph[node] = expand_node(graph, node)
    return new_graph

def print_graph(graph):
    for node in sorted(graph.keys()):
        print("{}\t{}".format(node, ','.join(sorted(graph[node]))))

def load_graph(filepath):
    graph = {}
    with open(filepath) as f:
        for line in f:
            node, targets = line.split('\t')
            graph[node] = targets.split(',')
    return graph

#-------------------------------------------------------------------------------

def load_diff_list(filepath):
    # Generate the input file by running:
    # git diff --name-only $commit1...$commit2 > $filepath

    with open(filepath) as f:
        return [line.strip() for line in f]

#-------------------------------------------------------------------------------

MVN_INFO_PREFIX = "[INFO] "

def read_mvn_log(filepath):
    with open(filepath) as f:
        for line in f:
            line = line.split(MVN_INFO_PREFIX, 1)
            if len(line) != 2:
                continue
            yield line[1].strip()

def load_mvn_dependencies_graph(filepath):
    # Generate the input file by running:
    # mvn dependency:list $PROFILE > $filepath

    # For example output snippet:
    # [INFO]
    # [INFO] --- maven-dependency-plugin:3.1.1:list (default-cli) @ flink-queryable-state-client-java_2.11 ---
    # [INFO]
    # [INFO] The following files have been resolved:
    # [INFO]    org.apache.flink:flink-core:jar:1.9-SNAPSHOT:provided
    # [INFO]    org.apache.flink:flink-annotations:jar:1.9-SNAPSHOT:provided
    # [INFO]    org.apache.flink:flink-metrics-core:jar:1.9-SNAPSHOT:provided
    # [INFO]    org.apache.flink:flink-shaded-asm-6:jar:6.2.1-7.0:provided
    # [INFO]    org.apache.flink:flink-shaded-netty:jar:4.1.32.Final-7.0:compile
    # [INFO]    org.apache.flink:flink-shaded-guava:jar:18.0-7.0:compile
    # [INFO]    org.apache.flink:flink-core:test-jar:tests:1.9-SNAPSHOT:test
    # [INFO]    org.apache.flink:flink-test-utils-junit:jar:1.9-SNAPSHOT:test
    # [INFO]    org.apache.flink:force-shading:jar:1.9-SNAPSHOT:compile
    # [INFO]
    #
    # should extract (key, value):
    # "flink-queryable-state-client-java_2.11": [
    #     "flink-core",
    #     "flink-annotations",
    #     "flink-metrics-core",
    #     "flink-core",
    #     "flink-test-utils-junit",
    #     "force-shading",
    # ]

    graph = defaultdict(set)

    mvn_artifact_pattern = re.compile(r"^-+ maven-dependency-plugin:(.*):list (.*) @ (.*) -+$")
    artifact = None
    dependencies = None
    for line in read_mvn_log(filepath):
        if artifact is None:
            match = mvn_artifact_pattern.match(line)
            if match:
                artifact = match.group(3)

        elif dependencies is None:
            if line == "The following files have been resolved:":
                dependencies = []

        else:
            if line:
                dependency = line.split(':')
                if dependency[0] == "org.apache.flink" and not dependency[1].startswith("flink-shaded-"):
                    # keep only artifactId part
                    dependencies.append(dependency[1])
            else:
                graph[artifact].update(dependencies)

                artifact = None
                dependencies = None

    return graph

def load_mvn_artifact_to_basedir(filepath):
    # Generate the input file by running:
    # mvn exec:exec -Dexec.executable="echo" -Dexec.args="[INFO] Basedir: ${project.basedir}" $PROFILE > $filepath

    # For example output snippet:
    # [INFO] --- exec-maven-plugin:1.6.0:exec (default-cli) @ flink-mapr-fs ---
    # [INFO] Basedir: /users/travis/build/42/flink/flink-filesystems/flink-mapr-fs
    #
    # should extract (key, value):
    # "flink-mapr-fs": "flink-filesystems/flink-mapr-fs"

    artifact_to_basedir = {}

    mvn_artifact_pattern = re.compile(r"^-+ exec-maven-plugin(.*)exec (.*) @ (.*) -+$")
    artifact = None
    for line in read_mvn_log(filepath):
        if artifact is None:
            match = mvn_artifact_pattern.match(line)
            if match:
                artifact = match.group(3)

        else:
            if line.startswith("Basedir: "):
                basedir = line[len("Basedir: "):]
                artifact_to_basedir[artifact] = basedir

                artifact = None

    # Make basedirs relative to project root.
    # We assume that the parent pom.xml is located in the root of the project tree,
    # and hence, it's basedir would be the project root.
    prefix = min(map(len, artifact_to_basedir.values()))
    for artifact in artifact_to_basedir.keys():
        basedir = artifact_to_basedir[artifact][prefix:]\
            .replace('\\', '/')\
            .lstrip('/')
        artifact_to_basedir[artifact] = basedir

    return artifact_to_basedir

#-------------------------------------------------------------------------------

def process_mvn_dependencies(mvn_dependencies_list_filepath, mvn_artifact_basedir_filepath):
    artifact_dependencies_graph = load_mvn_dependencies_graph(mvn_dependencies_list_filepath)
    artifact_dependencies_graph = expand_graph(artifact_dependencies_graph)

    artifact_to_basedir = load_mvn_artifact_to_basedir(mvn_artifact_basedir_filepath)

    basedir_dependencies_graph = defaultdict(set)
    for artifact, dependencies in artifact_dependencies_graph.items():
        basedir = artifact_to_basedir[artifact]
        for dependency in dependencies:
            basedir_dependencies_graph[basedir].add(artifact_to_basedir[dependency])

    print_graph(basedir_dependencies_graph)

def get_changed_parents(modules, diff_list):
    changed = []
    for filepath in diff_list:
        if filepath.endswith("/pom.xml") or filepath == "pom.xml":
            parent_module = filepath[:-len("pom.xml")].rstrip('/')
            if any(module.startswith(parent_module) for module in modules):
                changed.append(filepath)
    return changed

def get_changed_files(modules, diff_list):
    changed = []
    for filepath in diff_list:
        if any(filepath.startswith(module) for module in modules):
            changed.append(filepath)
    return changed

def can_skip_mvn_test(stage, profile, mvn_dependencies_tree_filepath, diff_list_filepath):
    if stage == STAGE_MISC:
        print("Cannot skip mvn test for stage '{}'".format(stage))
        return False

    mvn_dependencies_tree = load_graph(mvn_dependencies_tree_filepath)
    diff_list = load_diff_list(diff_list_filepath)

    modules = get_mvn_modules_for_test(stage, profile)
    dependencies = set()
    for module in modules:
        dependencies.update(mvn_dependencies_tree[module])

    changed = get_changed_parents(modules, diff_list)
    if changed:
        print("Cannot skip mvn test for stage '{}': some parents have changed\n\t{}".format(stage, "\n\t".join(changed)))
        return False

    changed = get_changed_parents(dependencies, diff_list)
    if changed:
        print("Cannot skip mvn test for stage '{}': some dependencies' parents have changed\n\t{}".format(stage, "\n\t".join(changed)))
        return False

    changed = get_changed_files(modules, diff_list)
    if changed:
        print("Cannot skip mvn test for stage '{}': some files have changed\n\t{}".format(stage, "\n\t".join(changed)))
        return False

    changed = get_changed_files(dependencies, diff_list)
    if changed:
        print("Cannot skip mvn test for stage '{}': some dependencies' files have changed\n\t{}".format(stage, "\n\t".join(changed)))
        return False

    return True

def main():
    argv = sys.argv
    command = argv[1]
    if command == "process-mvn-dependencies":
        process_mvn_dependencies(argv[2], argv[3])
        return

    if command == "can-skip-mvn-test":
        result = can_skip_mvn_test(argv[2], argv[3], argv[4], argv[5])
        sys.exit(0 if result else 1)

    print("Unknown command: {}".format(command), file=sys.stderr)
    sys.exit(1)

if __name__ == '__main__':
    main()
