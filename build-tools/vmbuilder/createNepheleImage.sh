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

# Must set this option, else script will not expand aliases.
shopt -s expand_aliases
source ~/.euca/eucarc

BASE=`dirname $0`/..

CONFIGDIR=$BASE/vmbuilder/config
TEMPLATEDIR=$BASE/vmbuilder/template
TARGETDIR=$BASE/target

CONFIGFILE=$CONFIGDIR/nfsNepheleImage.cfg

# set target location for the image
IMAGEDIR=$TARGETDIR/vmimage
MOUNTDIR=$IMAGEDIR/mnt

# set target location for the bundle
BUNDLEDIR=$TARGETDIR/vmbundle
# set kernel
# important: the kernel must be the same like in the config file
#BUNDLEKERNEL=eki-F4981533
BUNDLEKERNEL=eki-5BD71A4E
# set ramdisk
#BUNDLERAMDISK=eri-525E1685
BUNDLERAMDISK=eri-CCEA1B89
# set architecture
BUNDLEARCHITECTURE=x86_64
HYPERVISORNAME=xen
DISTRONAME=ubuntu
HYPERVISORFILE=$IMAGEDIR/root.img

TARGETFILE=ubuntu_jaunty_$BUNDLEARCHITECTURE.img
IMAGEFILE=$IMAGEDIR/$TARGETFILE
BUNDLEFILE=$BUNDLEDIR/$TARGETFILE.manifest.xml

# create target directory if it not exist
if [ ! -d $TARGETDIR ]; then
    mkdir $TARGETDIR
else
    rm -fR $TARGETDIR/vm*
fi

# check config file exist
if [ ! -f $CONFIGFILE ]; then
    echo cannot find $CONFIGFILE
    exit 1
fi

# to debug
#sudo vmbuilder xen ubuntu -v -o --debug -d $IMAGEDIR -c $CONFIGFILE --templates $TEMPLATEDIR
# create image
if [ ! -x vmbuilder ]; then
    echo sudo vmbuilder $HYPERVISORNAME $DISTRONAME -v -o -d $IMAGEDIR -c $CONFIGFILE --templates $TEMPLATEDIR
    sudo vmbuilder $HYPERVISORNAME $DISTRONAME -v -o -d $IMAGEDIR -c $CONFIGFILE --templates $TEMPLATEDIR
else
    echo vmbuilder $HYPERVISORNAME $DISTRONAME -v -o -d $IMAGEDIR -c $CONFIGFILE --templates $TEMPLATEDIR
    vmbuilder $HYPERVISORNAME $DISTRONAME -v -o -d $IMAGEDIR -c $CONFIGFILE --templates $TEMPLATEDIR
fi

# check image file exist
if [ ! -f $HYPERVISORFILE ]; then
    echo cannot find $HYPERVISORFILE
    exit 1
else
    mv $HYPERVISORFILE $IMAGEFILE
fi

# advanced configuration
mkdir $MOUNTDIR
# mount image
sudo mount -o loop $IMAGEFILE $MOUNTDIR
# mount proc
sudo mount -t proc none $MOUNTDIR/proc
# install java
sudo chroot $MOUNTDIR apt-get install --yes sun-java6-jre sun-java6-jdk
# install maven
# note: if it was installed before, open-jre would be used
sudo chroot $MOUNTDIR apt-get install --yes maven2 junit liblog4j1.2-java
# unmount proc
sudo umount $MOUNTDIR/proc

# rename fs
sudo mv $MOUNTDIR/dev/xvda $MOUNTDIR/dev/vda
sudo mv $MOUNTDIR/dev/xvda1 $MOUNTDIR/dev/vda1
sudo mv $MOUNTDIR/dev/xvda2 $MOUNTDIR/dev/vda2
sudo mv $MOUNTDIR/dev/xvda3 $MOUNTDIR/dev/vda3

# unmount image
sudo umount $MOUNTDIR

# create bundle directory if it not exist
if [ -d $BUNDLEDIR ]; then
    rm -fR $BUNDLEDIR
fi
mkdir $BUNDLEDIR

# create ec2 bundle
echo ec2-bundle-image -d $BUNDLEDIR -i $IMAGEFILE --arch $BUNDLEARCHITECTURE --kernel $BUNDLEKERNEL --ramdisk $BUNDLERAMDISK
ec2-bundle-image -d $BUNDLEDIR -i $IMAGEFILE --arch $BUNDLEARCHITECTURE --kernel $BUNDLEKERNEL --ramdisk $BUNDLERAMDISK

echo image and bundle creation complete
echo run: ec2-upload-bundle -b "bundle_bucket" -m $BUNDLEFILE
echo run: ec2-register "bundle_bucket"/$TARGETFILE.manifest.xml

exit 0

