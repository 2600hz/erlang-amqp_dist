### amqp erlang protocol distributor implementation

### compile
./rebar 3

### run

edit amqp uris in config/sys.config 
run: erl -pa _build/default/lib/*/ebin -proto_dist amqp -no_epmd -name node-01@your.host.com -setcookie change_me -config ./config/sys.config

### testing

start 2 or more nodes, in shell after 2/3 sec, type `nodes().`
you should see the nodes listed

play with it!

### adding a broker at runtime

> amqp_dist:add_broker("guest:guest@rabbitmq").


