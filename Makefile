REBAR := $(shell which rebar3 2>/dev/null || which ./rebar3)
SUBMODULES = apps/mg_proto/damsel
SUBTARGETS = $(patsubst %,%/.git,$(SUBMODULES))

REGISTRY := dr.rbkmoney.com
ORG_NAME := rbkmoney
BASE_IMAGE := "$(REGISTRY)/$(ORG_NAME)/build:latest"

RELNAME := machinegun

TAG = latest
IMAGE_NAME = "$(REGISTRY)/$(ORG_NAME)/$(RELNAME):$(TAG)"

CALL_ANYWHERE := submodules rebar-update compile xref lint dialyze start devrel release clean distclean

CALL_W_CONTAINER := $(CALL_ANYWHERE) test


# default
all: compile

# utils
include utils.mk

# build
$(SUBTARGETS): %/.git: %
	git submodule update --init $<
	touch $@

submodules: $(SUBTARGETS)

compile: submodules
	$(REBAR) compile

rebar-update:
	$(REBAR) update

devrel:
	$(REBAR) release

release: distclean
	$(REBAR) as prod release

# start
start: submodules devrel
	_build/default/rel/${RELNAME}/bin/${RELNAME} console


# clean
clean:
	$(REBAR) clean

distclean:
	$(REBAR) clean -a
	rm -rfv _build _builds _cache _steps _temp


# some checks
xref: submodules
	$(REBAR) xref

dialyze:
	$(REBAR) dialyzer

lint:
	elvis rock

test: submodules
	$(REBAR) ct


# containerize
containerize: w_container_release
	$(DOCKER) build --force-rm --tag $(IMAGE_NAME) .

push: containerize
	$(DOCKER) push "$(IMAGE_NAME)"


.PHONY: $(CALL_W_CONTAINER) all containerize push $(UTIL_TARGETS)
