#!/bin/sh

RELEASE_DIR="./releases/{{release_version}}"
YAML_CONFIG=${1:-./etc/config.yaml}
SYS_CONFIG="${RELEASE_DIR}/sys.config"
VM_ARGS="${RELEASE_DIR}/vm.args"
ERL_LIBS="lib" ./erts-{{release_erts_version}}/bin/escript bin/configurator.escript ${YAML_CONFIG} ${SYS_CONFIG} ${VM_ARGS} > /dev/null
exec ./bin/machinegun foreground
