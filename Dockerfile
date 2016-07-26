FROM rbkmoney/service_erlang:latest
MAINTAINER Petr Kozorezov <p.kozorezov@rbkmoney.com>
COPY ./_build/prod/rel/machinegun /opt/machinegun
CMD /opt/machinegun/bin/machinegun foreground
LABEL service_version="semver"
WORKDIR /opt/machinegun
