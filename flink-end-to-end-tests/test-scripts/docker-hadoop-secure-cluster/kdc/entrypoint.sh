#!/bin/bash -x
################################################################################
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

: "${KRB_REALM:=EXAMPLE.COM}"
: "${DOMAIN_REALM:=example.com}"
: "${KERB_MASTER_KEY:=masterkey}"
: "${KERB_ADMIN_USER:=admin}"
: "${KERB_ADMIN_PASS:=admin}"

create_config() {
  : "${KDC_ADDRESS:=$(hostname -f)}"

  cat>/etc/krb5.conf<<EOF
[logging]
 default = FILE:/var/log/kerberos/krb5libs.log
 kdc = FILE:/var/log/kerberos/krb5kdc.log
 admin_server = FILE:/var/log/kerberos/kadmind.log

[libdefaults]
 default_realm = ${KRB_REALM}
 dns_lookup_realm = false
 dns_lookup_kdc = false
 ticket_lifetime = 24h
 renew_lifetime = 7d
 forwardable = true

[realms]
 ${KRB_REALM} = {
  kdc = ${KDC_ADDRESS}
  admin_server = ${KDC_ADDRESS}
 }

[domain_realm]
 .$DOMAIN_REALM = ${KRB_REALM}
 $DOMAIN_REALM = ${KRB_REALM}
EOF
}

create_db() {
  /usr/sbin/kdb5_util -P ${KERB_MASTER_KEY} -r ${KRB_REALM} create -s
}

create_admin_user() {
  kadmin.local -q "addprinc -pw ${KERB_ADMIN_PASS} ${KERB_ADMIN_USER}/admin"
  echo "*/admin@${KRB_REALM} *" > /etc/krb5kdc/kadm5.acl
}

start_kdc() {
  /usr/sbin/krb5kdc
  /usr/sbin/kadmind
}

main() {
  mkdir -p /var/log/kerberos
  create_config
  create_db
  create_admin_user
  start_kdc
  tail -F /var/log/kerberos/krb5kdc.log
}

main "$@"
