# Flink-VM

This page provides instructions on how to automatically setup virtual machines running Apache Flink.

## Requirements

To run Flink-VM you need a recent version of vagrant (>= 1.7.4) and ansible (>= 2.0).

## Quick start

After downloading Flink-VM you can quickly get started:

~~~bash
cd flink-vm/single
vagrant up
~~~


Flink on a single VM is now up and running. You can access the Web Dashboard via http://localhost:8081

To run flink programs, enter the VM:

~~~bash
vagrant ssh
./wc-example.sh # script that runs the word count example
~~~

## Cluster setup

Exploring Flink on a virtual cluster is just as easy:

~~~bash
cd cluster
./setup.sh  # generate ssh keys
vagrant ssh
./wc-example.sh # script that runs the word count example
~~~

## Test Flink-VM

The tests should be run from a clean state. If you have instances of the VMs setup or running you should destroy them by running

~~~bash
vagrant destroy
~~~

in the respective directory.

### Single setup

Run the tests

~~~bash
cd flink-vm/single
vagrant up
ansible-playbook test.yml
~~~

Expected output

~~~bash
PLAY [Test the single VM setup] ************************************************

TASK [setup] *******************************************************************
ok: [default]

TASK [Test the VMs Flink instance by running the WordCount Example] ************
changed: [default]

PLAY RECAP *********************************************************************
default                    : ok=2    changed=1    unreachable=0    failed=0
~~~

### Cluster setup

Run the tests

~~~bash
cd flink-vm/cluster
./setup.sh
vagrant up
ansible-playbook test.yml
~~~

Expected output

~~~bash
PLAY [Test the cluster VM setup] ***********************************************

TASK [setup] *******************************************************************
ok: [master]

TASK [Test the VMs Flink instance by running the WordCount Example] ************
changed: [master]

PLAY RECAP *********************************************************************
master                     : ok=2    changed=1    unreachable=0    failed=0   
~~~
