#!/bin/bash
cat <<EOF
FROM $BASE_IMAGE
MAINTAINER Petr Kozorezov <p.kozorezov@rbkmoney.com>
COPY containerpilot.json /etc/containerpilot.json
COPY ./_build/prod/rel/machinegun /opt/machinegun
CMD /bin/containerpilot -config file:///etc/containerpilot.json /opt/machinegun/bin/machinegun foreground
EXPOSE 8022
LABEL base_image_tag=$BASE_IMAGE_TAG
LABEL build_image_tag=$BUILD_IMAGE_TAG
# A bit of magic to get a proper branch name
# even when the HEAD is detached (Hey Jenkins!
# BRANCH_NAME is available in Jenkins env).
LABEL branch=$( \
  if [ "HEAD" != $(git rev-parse --abbrev-ref HEAD) ]; then \
    echo $(git rev-parse --abbrev-ref HEAD); \
  elif [ -n "$BRANCH_NAME" ]; then \
    echo $BRANCH_NAME; \
  else \
    echo $(git name-rev --name-only HEAD); \
  fi)
LABEL commit=$(git rev-parse HEAD)
LABEL commit_number=$(git rev-list --count HEAD)
WORKDIR /opt
EOF
