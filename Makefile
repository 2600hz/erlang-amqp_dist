REBAR = $(shell which rebar3)

all:
	$(REBAR) $(MAKECMDGOALS)

clean:
	$(REBAR) clean

fmt:
	$(REBAR) fmt -w
