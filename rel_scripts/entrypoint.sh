#!/bin/sh

ROOT="$(dirname $0)/.."
RELEASE_DIR="${ROOT}/releases/{{release_version}}"
YAML_CONFIG=${1:-${ROOT}/etc/config.yaml}
SYS_CONFIG="${RELEASE_DIR}/sys.config"
VM_ARGS="${RELEASE_DIR}/vm.args"
ERL_LIBS="${ROOT}/lib" ${ROOT}/erts-{{release_erts_version}}/bin/escript ${ROOT}/bin/configurator.escript ${YAML_CONFIG} ${SYS_CONFIG} ${VM_ARGS} > /dev/null
exec ${ROOT}/bin/machinegun foreground
