REBAR = ./rebar3

all:
	@$(REBAR) $(MAKECMDGOALS)

%:
	@$(REBAR) $(MAKECMDGOALS)