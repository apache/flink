#!/usr/bin/env bash
########################################################################################################################
# Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
# an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.
########################################################################################################################

#
# See https://github.com/stratosphere/stratosphere/issues/95
# Inspired by: https://github.com/apache/hbase/blob/trunk/dev-support/generate-hadoopX-poms.sh
#

function usage {
  echo "Usage: $0 CURRENT_VERSION NEW_VERSION [POM_NAME]"
  echo "For example, $0 0.4-SNAPSHOT 0.4-hadoop2-SNAPSHOT"
  echo "Presumes VERSION has hadoop1 or hadoop2 in it. POM_NAME is optional and"
  echo "allows to specify a different name for the generated pom."
  exit 1
}

if [[ "$#" -lt 2 ]]; then usage; fi

old_version="$1"
new_version="$2"
new_pom_name="$3"

# Get hadoop version from the new stratosphere version
hadoop_version=`echo "$new_version" | sed -n 's/.*\(hadoop[12]\).*/\1/p'`
if [[ -z $hadoop_version ]]; then usage ; fi

echo "hadoop version $hadoop_version"


here="`dirname \"$0\"`"              # relative
here="`( cd \"$here\" && pwd )`"  # absolutized and normalized
if [ -z "$here" ] ; then
  # error; for some reason, the path is not accessible
  # to the script (e.g. permissions re-evaled after suid)
  exit 1  # fail
fi
stratosphere_home="`dirname \"$here\"`"


hadoop1=
hadoop2=
default='<name>!hadoop.profile<\/name>'
notdefault='<name>hadoop.profile<\/name>'
case "${hadoop_version}" in
  hadoop1)
    hadoop1="${default}"
    hadoop2="${notdefault}<value>2<\/value>"
    ;;
  hadoop2)
    hadoop1="${notdefault}<value>1<\/value>"
    hadoop2="${default}"
    ;;
 *) echo "Unknown ${hadoop_version}"
    usage
    ;;
esac

nupom=$new_pom_name
if [[ -z "$new_pom_name" ]]; then
  nupom="pom.${hadoop_version}.xml"
fi
echo "Using $nupom as name for the generated pom file."

poms=`find $stratosphere_home -name pom.xml`
for p in $poms; do
  # write into tmp file because in-place replacement is not possible (if nupom="pom.xml")
  tmp_nuname="`dirname $p`/__generate_specific_pom_tmp"
  nuname="`dirname $p`/${nupom}"
  # Now we do search and replace of explicit strings.  The best way of
  # seeing what the below does is by doing a diff between the original
  # pom and the generated pom (pom.hadoop1.xml or pom.hadoop2.xml). We
  # replace the version string in all poms, we change modules to
  # include reference to the non- standard pom name, we adjust
  # relative paths so child modules can find the parent pom, and we
  # enable/disable hadoop 1 and hadoop 2 profiles as appropriate
  # removing a comment string too. We output the new pom beside the
  # original.
  sed -e "s/${old_version}/${new_version}/" \
    -e "s/\(<module>[^<]*\)/\1\/${nupom}/" \
    -e "s/\(relativePath>\.\.\)/\1\/${nupom}/" \
    -e "s/<!--hadoop1-->.*name>.*/${hadoop1}/" \
    -e "s/<!--hadoop2-->.*<name>.*/${hadoop2}/" \
  $p > "$tmp_nuname"
  mv $tmp_nuname $nuname
done

