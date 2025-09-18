#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -e
set -o pipefail

if [ ${VERBOSE:-0} -gt 0 ]; then
  set -x
fi

case $# in
  0) VERSION="HEAD"
     SOURCE_KIND="local"
     TEST_BINARIES=0
     ;;
  1) VERSION="$1"
     SOURCE_KIND="git"
     TEST_BINARIES=0
     ;;
  2) VERSION="$1"
     RC_NUMBER="$2"
     SOURCE_KIND="tarball"
     ;;
  *) echo "Usage:"
     echo "  Verify release candidate:"
     echo "    $0 X.Y.Z RC_NUMBER"
     echo "  Verify only the source distribution:"
     echo "    TEST_DEFAULT=0 TEST_SOURCE=1 $0 X.Y.Z RC_NUMBER"
     echo ""
     echo "  Run the source verification tasks on a remote git revision:"
     echo "    $0 GIT-REF"
     echo "  Run the source verification tasks on this nanoarrow checkout:"
     echo "    $0"
     exit 1
     ;;
esac

# Note that these point to the current verify-release-candidate.sh directories
# which is different from the NANOARROW_SOURCE_DIR set in ensure_source_directory()
SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
NANOARROW_DIR="$(cd "${SOURCE_DIR}/../.." && pwd)"

show_header() {
  if [ -z "$GITHUB_ACTIONS" ]; then
    echo ""
    printf '=%.0s' $(seq ${#1}); printf '\n'
    echo "${1}"
    printf '=%.0s' $(seq ${#1}); printf '\n'
  else
    echo "::group::${1}"; printf '\n'
  fi
}

show_info() {
  echo "└ ${1}"
}

NANOARROW_DIST_URL='https://dist.apache.org/repos/dist/dev/arrow'

download_dist_file() {
  curl \
    --silent \
    --show-error \
    --fail \
    --location \
    --remote-name $NANOARROW_DIST_URL/$1
}

download_rc_file() {
  download_dist_file apache-arrow-nanoarrow-${VERSION}-rc${RC_NUMBER}/$1
}

import_gpg_keys() {
  if [ "${GPGKEYS_ALREADY_IMPORTED:-0}" -gt 0 ]; then
    return 0
  fi
  download_dist_file KEYS

  if [ "${NANOARROW_ACCEPT_IMPORT_GPG_KEYS_ERROR:-0}" -gt 0 ]; then
    gpg --import KEYS || true
  else
    gpg --import KEYS
  fi

  GPGKEYS_ALREADY_IMPORTED=1
}

if type shasum >/dev/null 2>&1; then
  sha512_verify="shasum -a 512 -c"
else
  sha512_verify="sha512sum -c"
fi

fetch_archive() {
  import_gpg_keys

  local dist_name=$1
  download_rc_file ${dist_name}.tar.gz
  download_rc_file ${dist_name}.tar.gz.asc
  download_rc_file ${dist_name}.tar.gz.sha512
  gpg --verify ${dist_name}.tar.gz.asc ${dist_name}.tar.gz
  ${sha512_verify} ${dist_name}.tar.gz.sha512
}

verify_dir_artifact_signatures() {
  import_gpg_keys

  # verify the signature and the checksums of each artifact
  find $1 -name '*.asc' | while read sigfile; do
    artifact=${sigfile/.asc/}
    gpg --verify $sigfile $artifact || exit 1

    # go into the directory because the checksum files contain only the
    # basename of the artifact
    pushd $(dirname $artifact)
    base_artifact=$(basename $artifact)
    if [ -f $base_artifact.sha256 ]; then
      ${sha256_verify} $base_artifact.sha256 || exit 1
    fi
    if [ -f $base_artifact.sha512 ]; then
      ${sha512_verify} $base_artifact.sha512 || exit 1
    fi
    popd
  done
}

setup_tempdir() {
  cleanup() {
    if [ "${TEST_SUCCESS}" = "yes" ]; then
      rm -fr "${NANOARROW_TMPDIR}"
    else
      echo "Failed to verify release candidate. See ${NANOARROW_TMPDIR} for details."
    fi
  }

  show_header "Creating temporary directory"

  if [ -z "${NANOARROW_TMPDIR}" ]; then
    # clean up automatically if NANOARROW_TMPDIR is not defined
    NANOARROW_TMPDIR=$(mktemp -d -t "nanoarrow-${VERSION}.XXXXXX")
    trap cleanup EXIT
  else
    # don't clean up automatically
    mkdir -p "${NANOARROW_TMPDIR}"
  fi

  echo "Working in sandbox ${NANOARROW_TMPDIR}"
}

setup_sedona_testing() {
  show_header "Setting up sedona-testing"

  if [ -z "${NANOARROW_ARROW_TESTING_DIR}" ]; then
    export NANOARROW_ARROW_TESTING_DIR="${NANOARROW_TMPDIR}/sedona-testing"
    git clone --depth=1 https://github.com/apache/sedona-testing ${NANOARROW_ARROW_TESTING_DIR}
  fi

  echo "Using arrow-testing at '${NANOARROW_ARROW_TESTING_DIR}'"
}

test_rust() {
  show_header "Build and test Rust libraries"

}

activate_or_create_venv() {
  if [ ! -z "${NANOARROW_PYTHON_VENV}" ]; then
    show_info "Activating virtual environment at ${NANOARROW_PYTHON_VENV}"
    source "${NANOARROW_PYTHON_VENV}/bin/activate"
  else
    # Try python3 first, then try regular python (e.g., already in a venv)
    if [ -z "${PYTHON_BIN}" ] && python3 --version >/dev/null; then
      PYTHON_BIN=python3
    elif [ -z "${PYTHON_BIN}" ]; then
      PYTHON_BIN=python
    fi

    show_info "Creating temporary virtual environment using ${PYTHON_BIN}..."
    "${PYTHON_BIN}" -m venv "${NANOARROW_TMPDIR}/venv"
    source "${NANOARROW_TMPDIR}/venv/bin/activate"
    python -m pip install --upgrade pip
  fi
}

test_python() {
  show_header "Build and test Python package"
  activate_or_create_venv

  pushd "${NANOARROW_SOURCE_DIR}/python"

  show_info "Installing Python package"
  rm -rf "${NANOARROW_TMPDIR}/python"
  pip install "sedonadb/[test]"

  show_info "Testing wheel"
  python -m pytest -vv

  popd
}

ensure_source_directory() {
  show_header "Ensuring source directory"

  dist_name="apache-arrow-nanoarrow-${VERSION}"

  if [ "${SOURCE_KIND}" = "local" ]; then
    # Local nanoarrow repository
    if [ -z "$NANOARROW_SOURCE_DIR" ]; then
      export NANOARROW_SOURCE_DIR="${NANOARROW_DIR}"
    fi
    echo "Verifying local nanoarrow checkout at ${NANOARROW_SOURCE_DIR}"
  else
    # Release tarball
    echo "Verifying official nanoarrow release candidate ${VERSION}-rc${RC_NUMBER}"
    export NANOARROW_SOURCE_DIR="${NANOARROW_TMPDIR}/${dist_name}"
    if [ ! -d "${NANOARROW_SOURCE_DIR}" ]; then
      pushd $NANOARROW_TMPDIR
      fetch_archive ${dist_name}
      tar xf ${dist_name}.tar.gz
      popd
    fi
  fi
}

test_source_distribution() {
  pushd $NANOARROW_SOURCE_DIR

  if [ ${TEST_RUST} -gt 0 ]; then
    test_rust
  fi

  if [ ${TEST_PYTHON} -gt 0 ]; then
    test_python
  fi

  popd
}

# By default test all functionalities.
# To deactivate one test, deactivate the test and all of its dependents
# To explicitly select one test, set TEST_DEFAULT=0 TEST_X=1
: ${TEST_DEFAULT:=1}

: ${TEST_SOURCE:=${TEST_DEFAULT}}
: ${TEST_RUST:=${TEST_SOURCE}}
: ${TEST_PYTHON:=${TEST_SOURCE}}

TEST_SUCCESS=no

setup_tempdir
setup_sedona_testing
ensure_source_directory
test_source_distribution

TEST_SUCCESS=yes

echo "Release candidate ${VERSION}-RC${RC_NUMBER} looks good!"
exit 0
