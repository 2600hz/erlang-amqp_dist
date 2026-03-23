REBAR = $(shell which rebar3)

compile:
	$(REBAR) compile

dialyzer:
	$(REBAR) dialyzer

clean:
	$(REBAR) clean

fmt:
	$(REBAR) fmt -w
