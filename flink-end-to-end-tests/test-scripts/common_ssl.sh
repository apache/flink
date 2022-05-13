#!/usr/bin/env bash

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

# NOTE: already sourced in common.sh

function _set_conf_ssl_helper {
    local type=$1 # 'internal' or external 'rest'
    local provider=$2 # 'JDK' or 'OPENSSL'
    local provider_lib=$3 # if using OPENSSL, choose: 'dynamic' or 'static' (how openSSL is linked to our packaged jar)
    local ssl_dir="${TEST_DATA_DIR}/ssl/${type}"
    local password="${type}.password"

    if [ "${type}" != "internal" ] && [ "${type}" != "rest" ]; then
        echo "Unknown type of ssl connectivity: ${type}. It can be either 'internal' or external 'rest'"
        exit 1
    fi
    if [ "${provider}" != "JDK" ] && [ "${provider}" != "OPENSSL" ]; then
        echo "Unknown SSL provider: ${provider}. It can be either 'JDK' or 'OPENSSL'"
        exit 1
    fi
    if [ "${provider_lib}" != "dynamic" ] && [ "${provider_lib}" != "static" ]; then
        echo "Unknown library type for openSSL: ${provider_lib}. It can be either 'dynamic' or 'static'"
        exit 1
    fi

    echo "Setting up SSL with: ${type} ${provider} ${provider_lib}"

    # clean up the dir that will be used for SSL certificates and trust stores
    if [ -e "${ssl_dir}" ]; then
       echo "File ${ssl_dir} exists. Deleting it..."
       rm -rf "${ssl_dir}"
    fi
    mkdir -p "${ssl_dir}"

    SANSTRING="dns:${NODENAME}"
    for NODEIP in $(get_node_ip) ; do
        SANSTRING="${SANSTRING},ip:${NODEIP}"
    done

    echo "Using SAN ${SANSTRING}"

    # create certificates
    keytool -genkeypair -alias ca -keystore "${ssl_dir}/ca.keystore" -dname "CN=Sample CA" -storepass ${password} -keypass ${password} -keyalg RSA -ext bc=ca:true -storetype PKCS12
    keytool -keystore "${ssl_dir}/ca.keystore" -storepass ${password} -alias ca -exportcert > "${ssl_dir}/ca.cer"
    keytool -importcert -keystore "${ssl_dir}/ca.truststore" -alias ca -storepass ${password} -noprompt -file "${ssl_dir}/ca.cer"

    keytool -genkeypair -alias node -keystore "${ssl_dir}/node.keystore" -dname "CN=${NODENAME}" -ext SAN=${SANSTRING} -storepass ${password} -keypass ${password} -keyalg RSA -storetype PKCS12
    keytool -certreq -keystore "${ssl_dir}/node.keystore" -storepass ${password} -alias node -file "${ssl_dir}/node.csr"
    keytool -gencert -keystore "${ssl_dir}/ca.keystore" -storepass ${password} -alias ca -ext SAN=${SANSTRING} -infile "${ssl_dir}/node.csr" -outfile "${ssl_dir}/node.cer"
    keytool -importcert -keystore "${ssl_dir}/node.keystore" -storepass ${password} -file "${ssl_dir}/ca.cer" -alias ca -noprompt
    keytool -importcert -keystore "${ssl_dir}/node.keystore" -storepass ${password} -file "${ssl_dir}/node.cer" -alias node -noprompt

    # keystore is converted into a pem format to use it as node.pem with curl in Flink REST API queries, see also $CURL_SSL_ARGS
    openssl pkcs12 -passin pass:${password} -in "${ssl_dir}/node.keystore" -out "${ssl_dir}/node.pem" -nodes

    if [ "${provider}" = "OPENSSL" -a "${provider_lib}" = "dynamic" ]; then
        cp $FLINK_DIR/opt/flink-shaded-netty-tcnative-dynamic-*.jar $FLINK_DIR/lib/
    elif [ "${provider}" = "OPENSSL" -a "${provider_lib}" = "static" ]; then
        # Flink is not providing the statically-linked library because of potential licensing issues
        # -> we need to build it ourselves
        FLINK_SHADED_VERSION=$(cat ${END_TO_END_DIR}/../pom.xml | sed -n 's/.*<flink.shaded.version>\(.*\)<\/flink.shaded.version>/\1/p')
        echo "BUILDING flink-shaded-netty-tcnative-static"
        # Adding retry to git clone, due to FLINK-24971
        retry_times_with_exponential_backoff 5 git clone https://github.com/apache/flink-shaded.git
        cd flink-shaded
        git checkout "release-${FLINK_SHADED_VERSION}"
        run_mvn clean package -Pinclude-netty-tcnative-static -pl flink-shaded-netty-tcnative-static
        cp flink-shaded-netty-tcnative-static/target/flink-shaded-netty-tcnative-static-*.jar $FLINK_DIR/lib/
        cd ..
        rm -rf flink-shaded
    fi

    # adapt config
    set_config_key security.ssl.provider ${provider}
    set_config_key security.ssl.${type}.enabled true
    set_config_key security.ssl.${type}.keystore ${ssl_dir}/node.keystore
    set_config_key security.ssl.${type}.keystore-password ${password}
    set_config_key security.ssl.${type}.key-password ${password}
    set_config_key security.ssl.${type}.truststore ${ssl_dir}/ca.truststore
    set_config_key security.ssl.${type}.truststore-password ${password}
}

function _set_conf_mutual_rest_ssl {
    local auth="${1:-server}" # only 'server' or 'mutual'
    local mutual="false"
    local ssl_dir="${TEST_DATA_DIR}/ssl/rest"
    if [ "${auth}" == "mutual" ]; then
        CURL_SSL_ARGS="${CURL_SSL_ARGS} --cert ${ssl_dir}/node.pem"
        mutual="true";
    fi
    echo "Mutual ssl auth: ${mutual}"
    set_config_key security.ssl.rest.authentication-enabled ${mutual}
}

function set_conf_rest_ssl {
    local auth="${1:-server}" # only 'server' or 'mutual'
    local provider="${2:-JDK}" # 'JDK' or 'OPENSSL'
    local provider_lib="${3:-dynamic}" # for OPENSSL: 'dynamic' or 'static'
    local ssl_dir="${TEST_DATA_DIR}/ssl/rest"
    _set_conf_ssl_helper "rest" "${provider}" "${provider_lib}"
    _set_conf_mutual_rest_ssl ${auth}
    REST_PROTOCOL="https"
    CURL_SSL_ARGS="${CURL_SSL_ARGS} --cacert ${ssl_dir}/node.pem"
}

function set_conf_ssl {
    local auth="${1:-server}" # only 'server' or 'mutual'
    local provider="${2:-JDK}" # 'JDK' or 'OPENSSL'
    local provider_lib="${3:-dynamic}" # for OPENSSL: 'dynamic' or 'static'
    _set_conf_ssl_helper "internal" "${provider}" "${provider_lib}"
    set_conf_rest_ssl ${auth} "${provider}" "${provider_lib}"
}

function rollback_openssl_lib() {
  rm $FLINK_DIR/lib/flink-shaded-netty-tcnative-{dynamic,static}-*.jar
}
