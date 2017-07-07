#!/bin/sh

ROOT="$(dirname $0)/.."
RELEASE_DIR="${ROOT}/releases/{{release_version}}"
YAML_CONFIG=${1:-${ROOT}/etc/config.yaml}
ERL_LIBS="${ROOT}/lib" ${ROOT}/erts-{{release_erts_version}}/bin/escript ${ROOT}/bin/configurator.escript ${YAML_CONFIG} ${RELEASE_DIR} > /dev/null
exec ${ROOT}/bin/machinegun foreground
