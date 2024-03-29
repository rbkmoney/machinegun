#
# Copyright 2020 RBKmoney
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
REBAR := $(shell which rebar3 2>/dev/null || which ./rebar3)
SUBMODULES = build_utils
SUBTARGETS = $(patsubst %,%/.git,$(SUBMODULES))

UTILS_PATH := build_utils
TEMPLATES_PATH := .

# Name of the service
SERVICE_NAME := machinegun
# Service image default tag
SERVICE_IMAGE_TAG ?= $(shell git rev-parse HEAD)
# The tag for service image to be pushed with
SERVICE_IMAGE_PUSH_TAG ?= $(SERVICE_IMAGE_TAG)

# Base image for the service
BASE_IMAGE_NAME := service-erlang
BASE_IMAGE_TAG := ef20e2ec1cb1528e9214bdeb862b15478950d5cd

# Build image tag to be used
BUILD_IMAGE_NAME := build-erlang
BUILD_IMAGE_TAG := aaa79c2d6b597f93f5f8b724eecfc31ec2e2a23b

CALL_ANYWHERE := \
	all \
	submodules \
	compile \
	xref \
	lint \
	dialyze \
	release \
	clean \
	distclean \
	test_configurator \
	format \
	check_format


CALL_W_CONTAINER := $(CALL_ANYWHERE) test dev_test test_configurator

all: compile

-include $(UTILS_PATH)/make_lib/utils_container.mk
-include $(UTILS_PATH)/make_lib/utils_image.mk

.PHONY: $(CALL_W_CONTAINER)

# CALL_ANYWHERE
$(SUBTARGETS): %/.git: %
	git submodule update --init $<
	touch $@

submodules: $(SUBTARGETS)

upgrade-proto:
	$(REBAR) upgrade mg_proto

compile: submodules
	$(REBAR) compile

xref: submodules
	$(REBAR) xref

lint:
	elvis rock -V

check_format:
	$(REBAR) fmt -c

format:
	$(REBAR) fmt -w

dialyze:
	$(REBAR) dialyzer

release:
	$(REBAR) as prod release

clean:
	$(REBAR) clean

distclean:
	rm -rfv _build

# CALL_W_CONTAINER
test: submodules test_configurator
	$(REBAR) ct

dev_test: xref lint test

test_configurator:
	$(MAKE) $(FILE_PERMISSIONS)
	ERL_LIBS=_build/default/lib ./rel_scripts/configurator.escript config/config.yaml config

FILE_PERMISSIONS = $(patsubst %,%.target,$(wildcard config/*._perms))
$(FILE_PERMISSIONS): config/%._perms.target: config/%._perms
	chmod $$(cat $^) config/$*
