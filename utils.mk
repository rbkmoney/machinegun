SHELL := /bin/bash

which = $(if $(shell which $(1) 2>/dev/null),\
	$(shell which $(1) 2>/dev/null),\
	$(error "Error: could not locate $(1)!"))

DOCKER = $(call which,docker)
DOCKER_COMPOSE = $(call which,docker-compose)

UTIL_TARGETS := to_dev_container w_container_% w_compose_% run_w_container_% check_w_container_%

ifndef RELNAME
$(error RELNAME is not set)
endif

ifndef CALL_W_CONTAINER
$(error CALL_W_CONTAINER is not set)
endif

to_dev_container:
	$(DOCKER) run -it --rm -v $$PWD:$$PWD --workdir $$PWD $(BASE_IMAGE) /bin/bash

w_container_%:
	$(MAKE) -s run_w_container_$*

w_compose_%:
	$(MAKE) -s run_w_compose_$*

run_w_container_%: check_w_container_%
	{ \
	$(DOCKER) run --rm -v $$PWD:$$PWD --workdir $$PWD $(BASE_IMAGE) make $* ; \
	res=$$? ; exit $$res ; \
	}

run_w_compose_%: check_w_container_%
	{ \
	$(DOCKER_COMPOSE) up -d ; \
	$(DOCKER_COMPOSE) exec -T $(RELNAME) make $* ; \
	res=$$? ; \
	$(DOCKER_COMPOSE) down ; \
	exit $$res ; \
	}

check_w_container_%:
	$(if $(filter $*,$(CALL_W_CONTAINER)),,\
	$(error "Error: target '$*' cannot be called w_container_"))
