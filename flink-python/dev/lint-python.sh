#!/usr/bin/env bash
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

# lint-python.sh
# This script will prepare a virtual environment for many kinds of checks, such as pytest, flake8 codestyle.
#
# You can refer to the README.MD in ${flink-python} to learn how easy to run the script.
#

# Download some software, such as miniconda.sh
function download() {
    local DOWNLOAD_STATUS=
    if hash "wget" 2>/dev/null; then
        wget "$1" -O "$2" -q --show-progress
        DOWNLOAD_STATUS="$?"
    else
        curl "$1" -o "$2" --progress-bar
        DOWNLOAD_STATUS="$?"
    fi
    if [ $DOWNLOAD_STATUS -ne 0 ]; then
        echo "Dowload failed.You can try again"
        exit $DOWNLOAD_STATUS
    fi
}

# for print infos both in log and console
function print_function() {
    local STAGE_LENGTH=48
    local left_edge_len=
    local right_edge_len=
    local str
    case "$1" in
        "STAGE")
            left_edge_len=$(((STAGE_LENGTH-${#2})/2))
            right_edge_len=$((STAGE_LENGTH-${#2}-left_edge_len))
            str="$(seq -s "=" $left_edge_len | tr -d "[:digit:]")""$2""$(seq -s "=" $right_edge_len | tr -d "[:digit:]")"
            ;;
        "STEP")
            str="$2"
            ;;
        *)
            str="seq -s "=" $STAGE_LENGTH | tr -d "[:digit:]""
            ;;
    esac
    echo $str | tee -a $LOG_FILE
}

# Checkpoint the stage:step for convenient to re-exec the script with
# skipping those success steps.
# The format is "${Stage}:${Step}". e.g. Install:4
function checkpoint_stage() {
    if [ ! -d `dirname $STAGE_FILE` ]; then
        mkdir -p `dirname $STAGE_FILE`
    fi
    echo "$1:$2">"$STAGE_FILE"
}

# Restore the stage:step
function restore_stage() {
    if [ -f "$STAGE_FILE" ]; then
        local lines=$(awk '{print NR}' $STAGE_FILE)
        if [ $lines -eq 1 ]; then
            local first_field=$(cat $STAGE_FILE | cut -d ":" -f 1)
            local second_field=$(cat $STAGE_FILE | cut -d ":" -f 2)
            check_valid_stage $first_field $second_field
            if [ $? -eq 0 ]; then
                STAGE=$first_field
                STEP=$second_field
                return
            fi
        fi
    fi
    STAGE="install"
    STEP=0
}

# Decide whether the stage:step is valid.
function check_valid_stage() {
    case $1 in
        "install")
            if [ $2 -le $STAGE_INSTALL_STEPS ] && [ $2 -ge 0 ]; then
                return 0
            fi
            ;;
        "tox")
            if [ $2 -eq 0 ]; then
                return 0
            fi
            ;;
        "flake8")
            if [ $2 -eq 0 ]; then
                return 0
            fi
            ;;
        *)
            ;;
    esac
    return 1
}

# For convenient to index something binded to OS.
# Now, the script only make a distinction between 'Mac' and 'Non-Mac'.
function get_os_index() {
    if [ $1 == "Darwin" ]; then
        return 0
    else
        return 1
    fi
}

# Considering the file size of miniconda.sh,
# "wget" is better than curl in the weak network environment.
function install_wget() {
    if [ $1 == "Darwin" ]; then
        hash "brew" 2>/dev/null
        if [ $? -ne 0 ]; then
            $((/usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)") 2>&1 >/dev/null)
            if [ $? -ne 0 ]; then
                echo "Failed to install brew"
                exit 1
            fi
        fi

        hash "wget" 2>/dev/null
        if [ $? -ne 0 ]; then
            brew install wget 2>&1 >/dev/null
            if [ $? -ne 0 ]; then
                echo "Failed to install wget"
                exit 1
            fi
        fi
    fi
}

# The script choose miniconda as our package management tool.
# The script use miniconda to create all kinds of python versions and
# some pakcages such as tox and flake8.

function install_miniconda() {
    OS_TO_CONDA_URL=("https://repo.continuum.io/miniconda/Miniconda3-latest-MacOSX-x86_64.sh" \
        "https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh")
    print_function "STEP" "download miniconda..."
    if [ ! -f "$CONDA_INSTALL" ]; then
        download ${OS_TO_CONDA_URL[$1]} $CONDA_INSTALL_SH
        chmod +x $CONDA_INSTALL_SH
        if [ $? -ne 0 ]; then
            echo "Please manually chmod +x $CONDA_INSTALL_SH"
            exit 1
        fi
        if [ -d "$CURRENT_DIR/.conda" ]; then
            rm -rf "$CURRENT_DIR/.conda"
            if [ $? -ne 0 ]; then
                echo "Please manually rm -rf $CURRENT_DIR/.conda directory.\
                Then retry to exec the script."
                exit 1
            fi
        fi
    fi
    print_function "STEP" "download miniconda... [SUCCESS]"

    print_function "STEP" "installing conda..."
    if [ ! -d "$CURRENT_DIR/.conda" ]; then
        $CONDA_INSTALL_SH -b -p $CURRENT_DIR/.conda 2>&1 >/dev/null
        if [ $? -ne 0 ]; then
            echo "install miniconda failed"
            exit $CONDA_INSTALL_STATUS
        fi
    fi
    print_function "STEP" "install conda ... [SUCCESS]"
}

# Install some kinds of py env.
function install_py_env() {
    py_env=("2.7" "3.3" "3.4" "3.5" "3.6" "3.7")
    for ((i=0;i<${#py_env[@]};i++)) do
        if [ -d "$CURRENT_DIR/.conda/envs/${py_env[i]}" ]; then
            rm -rf "$CURRENT_DIR/.conda/envs/${py_env[i]}"
            if [ $? -ne 0 ]; then
                echo "rm -rf $CURRENT_DIR/.conda/envs/${py_env[i]} failed, please \
                rm -rf $CURRENT_DIR/.conda/envs/${py_env[i]} manually.\
                Then retry to exec the script."
                exit 1
            fi
        fi
        print_function "STEP" "installing python${py_env[i]}..."
        ${CONDA_PATH} create --name ${py_env[i]} -y -q python=${py_env[i]} 2>&1 >/dev/null
        if [ $? -ne 0 ]; then
            echo "conda install ${py_env[i]} failed.\
            You can retry to exec the script."
            exit 1
        fi
        print_function "STEP" "install python${py_env[i]}... [SUCCESS]"
    done
}

# Install tox.
# In some situations,you need to run the script with "sudo". e.g. sudo ./lint-python.sh
function install_tox() {
    if [ -f "$TOX_PATH" ]; then
        ${CONDA_PATH} remove tox -y -q 2>&1 >/dev/null
        if [ $? -ne 0 ]; then
            echo "conda remove tox failed \
            please try to exec the script again.\
            if failed many times, you can try to exec in the form of sudo ./lint-python.sh -f"
            exit 1
        fi
    fi

    ${CONDA_PATH} install -c conda-forge tox -y -q 2>&1 >/dev/null
    if [ $? -ne 0 ]; then
        echo "conda install tox failed \
        please try to exec the script again.\
        if failed many times, you can try to exec in the form of sudo ./lint-python.sh -f"
        exit 1
    fi
}

# Install flake8.
# In some situations,you need to run the script with "sudo". e.g. sudo ./lint-python.sh
function install_flake8() {
    if [ -f "$FLAKE8_PATH" ]; then
        ${CONDA_PATH} remove flake8 -y -q 2>&1 >/dev/null
        if [ $? -ne 0 ]; then
            echo "conda remove flake8 failed \
            please try to exec the script again.\
            if failed many times, you can try to exec in the form of sudo ./lint-python.sh -f"
            exit 1
        fi
    fi

    ${CONDA_PATH} install -c anaconda flake8 -y -q 2>&1 >/dev/null
    if [ $? -ne 0 ]; then
        echo "conda install flake8 failed \
        please try to exec the script again.\
        if failed many times, you can try to exec in the form of sudo ./lint-python.sh -f"
        exit 1
    fi
}


# In this function, the script will prepare all kinds of python environments and checks.
function install_environment() {

    print_function "STAGE" "Stage 1:installing environment"

    local sys_os=`uname -s`
    #get the index of the SUPPORT_OS array for convinient to intall tool.
    get_os_index $sys_os
    local os_index=$?

    # step-1 install wget
    # the file size of the miniconda.sh is too big to use "wget" tool to download instead
    # of the "curl" in the weak network environment.
    print_function "STEP" "installing wget..."
    if [ $STEP -lt 1 ]; then
        install_wget ${SUPPORT_OS[$os_index]}
        STEP=1
        checkpoint_stage $STAGE $STEP
    fi
    print_function "STEP" "install wget... [SUCCESS]"

    # step-2 install miniconda
    print_function "STEP" "installing miniconda..."
    if [ $STEP -lt 2 ]; then
        create_dir $CURRENT_DIR/download
        install_miniconda $os_index
        STEP=2
        checkpoint_stage $STAGE $STEP
    fi
    print_function "STEP" "install miniconda... [SUCCESS]"

    # step-3 install python environment whcih includes
    # 2.7 3.3 3.4 3.5 3.6 3.7
    print_function "STEP" "installing python environment..."
    if [ $STEP -lt 3 ]; then
        install_py_env
        STEP=3
        checkpoint_stage $STAGE $STEP
    fi
    print_function "STEP" "install python environment... [SUCCESS]"

    # step-4 install tox
    print_function "STEP" "installing tox..."
    if [ $STEP -lt 4 ]; then
        install_tox
        STEP=4
        checkpoint_stage $STAGE $STEP
    fi
    print_function "STEP" "install tox... [SUCCESS]"

    # step-5 install  flake8
    print_function "STEP" "installing flake8..."
    if [ $STEP -lt 5 ]; then
        install_flake8
        STEP=5
        checkpoint_stage $STAGE $STEP
    fi
    print_function "STEP" "install flake8... [SUCCESS]"

    print_function "STAGE"  "Stage 1:finishing install environment"

    STAGE="tox"
    STEP=0
}

# create dir if needed
function create_dir() {
    if [ ! -d $1 ]; then
        mkdir -p $1
        if [ $? -ne 0 ]; then
            echo "mkdir -p $1 failed. you can mkdir manually or exec the script with \
            the command: sudo ./lint-python.sh"
            exit 1
        fi
    fi
}

# Set created py-env in $PATH for tox's creating virtual env
function activate () {
    if [ ! -d ${CURRENT_DIR}/.conda/envs ]; then
        echo "For some unkown reasons,missing the directory ${CURRENT_DIR}/.conda/envs,\
        you should exec the script with the parameter: -f"
        exit 1
    fi

    for py_dir in ${CURRENT_DIR}/.conda/envs/*
    do
        PATH=$py_dir/bin:$PATH
    done
    export PATH 2>/dev/null
    if [ $? -ne 0 ]; then
        echo "For some unkown reasons, the py package is not complete,\
        you should exec the script with the parameter: -f"
        exit 1
    fi
}

# Reset the $PATH
function deactivate() {
    # reset old environment variables
    # ! [ -z ${VAR+_} ] returns true if VAR is declared at all
    if ! [ -z "${_OLD_PATH+_}" ] ; then
        PATH="$_OLD_PATH"
        export PATH
        unset _OLD_PATH
    fi
}

# Tox pytest Check
function tox_check() {
    print_function "STAGE" "Stage 2:tox check"
    # Set created py-env in $PATH for tox's creating virtual env
    activate
    $TOX_PATH -c $FLINK_PYTHON_DIR/tox.ini --recreate 2>&1 | tee -a $LOG_FILE

    TOX_RESULT=$((grep -c "congratulations :)" "$LOG_FILE") 2>&1)
    if [ $TOX_RESULT -eq '0' ]; then
        print_function "STAGE" "tox checked ... [FAILED]"
    else
        print_function "STAGE" "tox checked ... [SUCCESS]"
    fi
    # Reset the $PATH
    deactivate
    # If check failed, stop the running script.
    if [ $TOX_RESULT -eq '0' ]; then
        exit 1
    fi
    STAGE="flake8"
    STEP="0"
}

# Flake8 codestyle check
function flake8_check {
    local PYTHON_SOURCE="$(find . \( -path ./dev -o -path ./.tox \) -prune -o -type f -name "*.py" -print )"

    print_function "STAGE" "Stage 3:flake8 code style check"
    if [ ! -f "$FLAKE8_PATH" ]; then
        echo "For some unkown reasons, the flake8 package is not complete,\
        you should exec the script with the parameter: -f"
    fi

    if [[ ! "$PYTHON_SOURCE" ]]; then
        echo "No python files found!  Something is wrong exiting."
        exit 1;
    fi

    # the return value of a pipeline is the status of the last command to exit
    # with a non-zero status or zero if no command exited with a non-zero status
    set -o pipefail
    ($FLAKE8_PATH  --config=tox.ini $PYTHON_SOURCE) 2>&1 | tee -a $LOG_FILE

    PYCODESTYLE_STATUS=$?
    if [ $PYCODESTYLE_STATUS -ne 0 ]; then
        print_function "STAGE" "python code style checks ... [FAILED]"
        # Stop the running script.
        exit 1;
    else
        print_function "STAGE" "python code style checks ... [SUCCESS]"
    fi
}

# CURRENT_DIR is "flink/flink-python/dev/"
CURRENT_DIR="$(cd "$( dirname "$0" )" && pwd)"

# FLINK_PYTHON_DIR is "flink/flink-python"
FLINK_PYTHON_DIR=$(dirname "$CURRENT_DIR")
pushd "$FLINK_PYTHON_DIR" &> /dev/null

# conda path
CONDA_PATH=$CURRENT_DIR/.conda/bin/conda

# tox path
TOX_PATH=$CURRENT_DIR/.conda/bin/tox

# flake8 path
FLAKE8_PATH=$CURRENT_DIR/.conda/bin/flake8

_OLD_PATH="$PATH"

SUPPORT_OS=("Darwin" "Linux")

# the file stores the success progress.
STAGE_FILE=$CURRENT_DIR/.stage.txt

# the dir includes all kinds of py env installed.
VIRTUAL_ENV=$CURRENT_DIR/.conda/envs

LOG_DIR=$CURRENT_DIR/log

if [ "$FLINK_IDENT_STRING" == "" ]; then
    FLINK_IDENT_STRING="$USER"
fi
if [ "$HOSTNAME" == "" ]; then
    HOSTNAME="$HOST"
fi

# the log file stores the checking result.
LOG_FILE=$LOG_DIR/flink-$FLINK_IDENT_STRING-python-$HOSTNAME.log
create_dir $LOG_DIR

# clean LOG_FILE content
echo >$LOG_FILE

# miniconda script
CONDA_INSTALL_SH=$CURRENT_DIR/download/miniconda.sh

# stage "install" includes the num of steps.
STAGE_INSTALL_STEPS=5

# whether force to restart the script.
FORCE_START=0
# parse_opts
USAGE="
usage: $0 [options]$
-h           print this help message and exit
-f           force to exec from the start
"
while getopts "hf" arg; do
    case "$arg" in
        h)
            printf "%s\\n" "$USAGE"
            exit 2
            ;;
        f)
            FORCE_START=1
            ;;
        ?)
            printf "ERROR: did not recognize option '%s', please try -h\\n" "$1"
            exit 1
            ;;
    esac
done
# If exec the script with the param: -f, all progress will be re-run
if [ $FORCE_START -eq 1 ]; then
    STAGE="install"
    STEP=0
    checkpoint_stage $STAGE $STEP
else
    restore_stage
fi
# Stage 1:install environment
if [ $STAGE == "install" ]; then
    install_environment
fi
# Stage 2:tox check python compatibility
if [ $STAGE == "tox" ]; then
    tox_check
fi
# Stage 3:code style test
if [ $STAGE == "flake8" ]; then
    flake8_check
fi

echo "All the checks are finished, the detailed information can be found in: $LOG_FILE"
