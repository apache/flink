#!/usr/bin/env sh

#
# See https://github.com/dimalabs/ozone/issues/95
# Inspired by: https://github.com/apache/hbase/blob/trunk/dev-support/generate-hadoopX-poms.sh
#


function usage {
  echo "Usage: $0 CURRENT_VERSION NEW_VERSION"
  echo "For example, $0 0.2-ozone 0.2-ozone-hadoop2"
  echo "Presumes VERSION has hadoop1 or hadoop2 in it."
  exit 1
}

if [[ "$#" -ne 2 ]]; then usage; fi

old_ozone_version="$1"
new_ozone_version="$2"

# Get hadoop version from the new ozone version
hadoop_version=`echo "$new_ozone_version" | sed -n 's/.*\(hadoop[12]\).*/\1/p'`
if [[ -z $hadoop_version ]]; then usage ; fi

echo "hadoop version $hadoop_version"
here="`dirname \"$0\"`"              # relative
here="`( cd \"$here\" && pwd )`"  # absolutized and normalized
if [ -z "$here" ] ; then
	# error; for some reason, the path is not accessible
	# to the script (e.g. permissions re-evaled after suid)
	exit 1  # fail
fi
ozone_home="`dirname \"$here\"`"



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


nupom="pom.${hadoop_version}.xml"
poms=`find $ozone_home -name pom.xml`
for p in $poms; do
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
  sed -e "s/${old_ozone_version}/${new_ozone_version}/" \
    -e "s/\(<module>[^<]*\)/\1\/${nupom}/" \
    -e "s/\(relativePath>\.\.\)/\1\/${nupom}/" \
    -e "s/<!--hadoop1-->.*name>.*/${hadoop1}/" \
    -e "s/<!--hadoop2-->.*<name>.*/${hadoop2}/" \
  $p > "$nuname"
done

