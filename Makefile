REBAR := $(shell which rebar3 2>/dev/null || which ./rebar3)
RELNAME = mg

.PHONY: all compile devrel start test clean distclean dialyze

all: compile

compile:
	$(REBAR) compile

rebar-update:
	$(REBAR) update

devrel:
	$(REBAR) release

start: devrel
	_build/default/rel/${RELNAME}/bin/${RELNAME} console

test:
	$(REBAR) ct

xref:
	$(REBAR) xref

clean:
	$(REBAR) clean

distclean:
	$(REBAR) clean -a
	rm -rfv _build _builds _cache _steps _temp

dialyze:
	$(REBAR) dialyzer

lint:
	elvis rock
