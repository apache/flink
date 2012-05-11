########################################################################################################################
# 
#  Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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

# This script will run the first time the vm boots.
# It is ran as root.

echo starting user boot script

# Expire the user account for nephele
# passwd -e nephele

# Regenerate ssh keys
rm /etc/ssh/ssh_host_*
dpkg-reconfigure openssh-server

# create nfs director
NEPHELEHOME=/home/nephele
if [ ! -d $NEPHELEHOME ]; then
    mkdir $NEPHELEHOME
fi

# mout the directory
mount 192.168.198.8:$NEPHELEHOME $NEPHELEHOME

# more

# check ip
ifconfig

