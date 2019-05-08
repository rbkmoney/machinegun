#!/bin/sh
#
# Copyright 2017 RBKmoney
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -e

ROOT="$(dirname $0)/.."
RELEASE_DIR="${ROOT}/releases/{{release_version}}"
YAML_CONFIG=${1:-${ROOT}/etc/config.yaml}
ERL_LIBS="${ROOT}/lib" ${ROOT}/erts-{{release_erts_version}}/bin/escript ${ROOT}/bin/configurator.escript ${YAML_CONFIG} ${RELEASE_DIR} > /dev/null
exec ${ROOT}/bin/machinegun foreground
