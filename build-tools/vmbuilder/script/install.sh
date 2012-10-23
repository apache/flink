########################################################################################################################
# 
#  Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
# 
#  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
#  the License. You may obtain a copy of the License at
# 
#      http://www.apache.org/licenses/LICENSE-2.0
# 
#  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
#  an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
#  specific language governing permissions and limitations under the License.
# 
########################################################################################################################

#!/bin/bash

# Set this script as argument in vmbuilder: --execscript=SCRIPT
# This script will run after the distro installation.
# It is ran as root.

#chroot $1 echo "starting user install script"

# install java
chroot $1 apt-get install -qqy --force-yes sun-java6-jre sun-java6-jdk
# install maven
# note: if it was installed before, open-jre would be used
chroot $1 apt-get install -qqy maven2 junit liblog4j1.2-java


