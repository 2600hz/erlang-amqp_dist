-module(amqp_dist_acceptor).

-include("amqp_dist.hrl").

-behaviour(gen_server).

-export([init/1
        ,handle_call/3
        ,handle_cast/2
        ,handle_info/2
        ,terminate/2
        ,code_change/3
        ]).

-compile({no_auto_import,[nodes/0]}).

%% ====================================================================
%% API functions
%% ====================================================================
-export([start/2, start_link/0]).

-export([add_broker/1]).
-export([nodes/0]).
-export([is_up/1, connect/1, accept/1]).
-export([stop/0, stop/1]).
-export([acceptor/1, acceptor/2]).


-define(CONNECTION_TIMEOUT, 10000).
-define(HEARTBEAT_PERIOD, 3500).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [undefined], []).

start(Kernel, Name) ->
    gen_server:start({local, ?MODULE}, ?MODULE, [Kernel, Name], []).

add_broker(Uri) ->
    case catch amqp_uri:parse(Uri) of
        {'EXIT', _R} ->
            lager:error("failed to parse AMQP URI '~s': ~p", [Uri, _R]),
            {'error', 'invalid_uri'};
        {'error', {Info, _}} ->
            lager:error("failed to parse AMQP URI '~s': ~p", [Uri, Info]),
            {'error', 'invalid_uri'};
        {'ok', Params} ->
            gen_server:call(?MODULE, {add_broker, Uri, Params})
    end.

init([Kernel, Name]) ->
    link(Kernel),
    process_flag(trap_exit, true),
    erlang:send_after(6000, self(), expire),
    Env = application:get_all_env(amqp_dist),
    self() ! 'start',
    Node = list_to_binary([atom_to_binary(Name, utf8), "@"
                          ,inet_db:gethostname(), ".", inet_db:res_option(domain)
                          ]),
    {ok, #{kernel => Kernel
          ,nodes => #{}
          ,connections => #{}
          ,pids => #{}
          ,tags => #{}
          ,this => Node
          ,env => Env
          ,node_started_at => os:system_time(microsecond)
          }
    }.

handle_call({add_broker, Uri, Params}, _From, State) ->
    case start_broker(Uri, Params, State) of
        {ok, starting} -> {reply, ok, State};
        {error, _Error} = Err -> {reply, Err, State}
    end;

handle_call(nodes, _From, #{nodes := Nodes} = State) ->
    {reply, Nodes, State};

handle_call(state, _From, State) ->
    {reply, State, State};

handle_call(stop, _From, State) ->
    {stop, normal, State};

handle_call({acceptor, Acceptor}, _From, State) ->
    {reply, ok, State#{acceptor => Acceptor}};

handle_call({is_up, Node}, _From, #{nodes := Nodes} = State) ->
    case maps:get(Node, Nodes, undefined) of
        undefined -> {reply, false, State};
        _ ->  {reply, true, State}
    end;

handle_call({connection, Node}, _From, #{nodes := Nodes, connections := Connections} = State) ->
    case maps:get(Node, Nodes, undefined) of
        undefined ->
            {reply, {error, not_available}, State};
        Map ->
            {Uri, #{queue := Queue}} = best_node(Map),
            #{connection := Pid} = maps:get(Uri, Connections),
            {reply, {ok, {Pid, Queue}}, State}
    end;

handle_call(_Request, _From, State) ->
    {reply, ok, State}.


handle_cast(_Msg, State) ->
    {noreply, State}.


handle_info(expire, #{nodes := Nodes} = State) ->
    NewNodes = expire(Nodes),
    erlang:send_after(3000, self(), expire),
    {noreply, State#{nodes => NewNodes}};

handle_info({#'basic.deliver'{}
            ,#amqp_msg{props = #'P_basic'{correlation_id = This}}
            }
            ,State = #{this := This}) ->
    {noreply, State};

%% @private
handle_info({#'basic.deliver'{consumer_tag = Tag
                             ,exchange = <<"amq.headers">>
                             }
            ,#amqp_msg{props = #'P_basic'{correlation_id = Id
                                         ,reply_to = Queue
                                         ,timestamp = Published
                                         ,headers = Headers
                                         }
                      }
            }
            ,State = #{nodes := Nodes, node_started_at := LocalStarted}) ->
    try
        {<<"node.start">>, _, RemoteStarted} = lists:keyfind(<<"node.start">>, 1, Headers),
        #{tags := #{Tag := Uri}} = State,
        Node = binary_to_atom(Id, utf8),
        Received = os:system_time(microsecond),
        Latency = Received - Published,
        NodeData = maps:get(Node, Nodes, #{}),
        NodeUris = maps:get(uris, NodeData, #{}),
        Data = #{time => erlang:system_time(millisecond)
                ,queue => Queue
                ,latency => Latency
                },
        maybe_connect(maps:size(NodeData), {Node, RemoteStarted, LocalStarted}),
        {noreply, State#{nodes => Nodes#{Node => NodeData#{uris => NodeUris#{Uri => Data}, node_started_at => RemoteStarted}}}}
    catch
        _E:_R -> {noreply, State}
    end;

handle_info({#'basic.deliver'{consumer_tag = Tag
                              ,exchange = <<>>
                              ,routing_key = Queue
                             }
             ,#amqp_msg{props = #'P_basic'{correlation_id = NodeId
                                           ,reply_to = RemoteQueue
                                          }
                        ,payload = Payload
                       }
            }
            ,State = #{acceptor := Acceptor}) ->
    try
        #{tags := #{Tag := Uri}} = State,
        #{connections := #{Uri := #{queue := Queue
                                   ,connection := Connection
                                   }
                          }
         } = State,
        Node = binary_to_atom(NodeId, utf8),
        {amqp_dist, connect} = decode(Payload),
        Acceptor ! {connection, Node, Connection, RemoteQueue},
        {noreply, State}
    catch
        _E:_R -> {noreply, State}
    end;


handle_info({#'basic.return'{}, _}, State) ->
    {noreply, State};

handle_info({'heartbeat', Ref, Uri}, #{connections := Connections} = State) ->
    case maps:get(Uri, Connections, undefined) of
        undefined -> {noreply, State};
        #{heartbeat := Ref}=Broker ->
            publish(Broker),
            Reference = erlang:make_ref(),
            _ = erlang:send_after(?HEARTBEAT_PERIOD, self(), {'heartbeat', Reference, Uri}),
            {noreply, State#{connections => Connections#{Uri => Broker#{heartbeat => Reference}}}};
        _Other -> {noreply, State}
    end;

handle_info('start', State) ->
    spawn(fun() -> start_connections(State) end),
    {noreply, State};

handle_info({#'basic.consume'{}, _Pid}, State) ->
    {noreply, State};

%% @private
handle_info(#'basic.consume_ok'{}, State) ->
    {noreply, State};

handle_info(#'basic.cancel'{consumer_tag = Tag, nowait = _NoWait}, #{tags := Tags} = State) ->
    case maps:get(Tag, Tags, undefined) of
        undefined -> {noreply, State};
        Uri -> {noreply, remove(Uri, State)}
    end;

handle_info({'DOWN', Ref, process, _Pid, _Reason}, #{refs := Refs} = State) ->
    case maps:get(Ref, Refs, undefined) of
        undefined -> {noreply, State};
        Uri -> {noreply, remove(Uri, State)}
    end;

handle_info({'EXIT', Pid, _Reason}, #{pids := Pids} = State) ->
    case maps:get(Pid, Pids, undefined) of
        undefined -> {noreply, State};
        Uri -> {noreply, remove(Uri, State)}
    end;

handle_info({reconnect, Uri, Params}, State) ->
    start_broker(Uri, Params, State),
    {noreply, State};

handle_info({started, #{uri := Uri
                       ,connection := Connection
                       ,channel := Channel
                       } = Broker0}, State) ->
    Connections = maps:get(connections, State, #{}),
    Tags = maps:get(tags, State, #{}),
    Refs = maps:get(refs, State, #{}),
    Pids = maps:get(pids, State, #{}),
    Broker = #{consumer_tag := Tag
              ,connection_ref := ConnectionRef
              ,channel_ref := ChannelRef
              } = start_heartbeat(Broker0),

    {noreply, State#{connections => Connections#{Uri => Broker}
                    ,tags => Tags#{Tag => Uri}
                    ,refs => Refs#{ConnectionRef => Uri
                                  ,ChannelRef => Uri
                                  }
                    ,pids => Pids#{Connection => Uri
                                  ,Channel => Uri
                                  }
                    }
    };

handle_info(_Info, State) ->
    lager:info("UNHANDLED MSG : ~p => ~p", [_Info, State]),
    {noreply, State}.

terminate('shutdown', _State) -> ok;
terminate('killed', _State) -> ok;
terminate(_Reason, #{connections := Connections}=_State) ->
    catch(stop_connections(Connections)),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

acceptor(Acceptor) ->
    gen_server:call(?MODULE, {acceptor, Acceptor}).

acceptor(Pid, Acceptor) ->
    gen_server:call(Pid, {acceptor, Acceptor}).

nodes() ->
   gen_server:call(?MODULE, nodes).

expire(Nodes) ->
    Now = erlang:system_time(millisecond),
    {Now, ALive} = maps:fold(fun expire_node/3, {Now, #{}}, Nodes),
    ALive.

expire_node(Node, Data, {Now, Nodes}=Acc) ->
    {Now, NewConnections} = maps:fold(fun expire_connection/3, {Now, #{}}, maps:get(uris, Data, #{})),
    case maps:size(NewConnections) of
        0 -> Acc;
        _ -> {Now, Nodes#{Node => Data#{uris => NewConnections}}}
    end.

expire_connection(Pid, #{time := Time}=Connection, {Now, Connections}=Acc) ->
    case Now - Time > ?CONNECTION_TIMEOUT of
        true -> Acc;
        false -> {Now, Connections#{Pid => Connection}}
    end.

is_up(Node) ->
    case whereis(?MODULE) of
        undefined -> false;
        Pid when is_pid(Pid) ->
            case is_process_alive(Pid) of
                true -> gen_server:call(Pid, {is_up, Node});
                false -> false
            end;
        _Else -> false
    end.

connect(Node) ->
    case gen_server:call(?MODULE, {connection, Node}) of
        {ok, {Pid, Queue}} -> amqp_dist_node:start(Pid, Node, Queue, 'connect');
        Error -> Error
    end.

best_node(Map) ->
    hd(lists:sort(fun best_node/2, maps:to_list(maps:get(uris, Map, #{})))).

best_node({_, #{latency := L1}}, {_, #{latency := L2}}) ->
    L2 > L1.

accept({Node, Connection, Queue}) ->
    amqp_dist_node:start(Connection, Node, Queue, 'accept').

open_channel(Broker = #{connection := Connection, server := Pid}) ->
    {ok, Channel} = amqp_connection:open_channel(
                        Connection, {amqp_direct_consumer, [Pid]}),
    ChannelRef = erlang:monitor(process, Channel),
    ConnectionRef = erlang:monitor(process, Connection),
    
    Broker#{channel => Channel
           ,channel_ref => ChannelRef
           ,connection_ref => ConnectionRef
           }.

set_exchange(Broker) ->
    Broker#{exchange => <<"amq.headers">>}.

declare_queue(Broker = #{channel := Channel}) ->
    #'queue.declare_ok'{queue = Q} =
        amqp_channel:call(Channel, #'queue.declare'{exclusive   = true,
                                                    auto_delete = true}),
    Broker#{queue => Q}.

bind_queue(Broker = #{channel := Channel, exchange := Exchange, queue := Q}) ->
    #'queue.bind_ok'{} =
        amqp_channel:call(Channel, #'queue.bind'{queue = Q
                                                ,exchange = Exchange
                                                ,arguments = [{<<"distribution.ping">>, bool, true}]
                                                }),
    Broker.

consume_queue(Broker = #{channel := Channel, queue := Q}) ->
    #'basic.consume_ok'{consumer_tag = ConsumerTag} =
        amqp_channel:call(Channel, #'basic.consume'{queue = Q, no_ack = true, no_local = true}),
    Broker#{consumer_tag => ConsumerTag}.

return_handler(#{channel := Channel, server := Server}) ->
    amqp_channel:register_return_handler(Channel, Server).

start_heartbeat(Broker = #{uri := Uri, connection := Connection, channel := Channel}) ->
    ConnectionRef = erlang:monitor(process, Connection),
    ChannelRef = erlang:monitor(process, Channel),
    Reference = erlang:make_ref(),
    erlang:send_after(500, self(), {'heartbeat', Reference, Uri}),
    Broker#{heartbeat => Reference, connection_ref => ConnectionRef, channel_ref => ChannelRef}.

start_broker(Uri, Params, #{node_started_at := Start, connections := Connections}) ->
    case maps:get(Uri, Connections, undefined) of
        undefined ->
            case amqp_connection:start(Params) of
                {ok, Pid} ->
                        Broker = #{params => Params
                                  ,connection => Pid
                                  ,uri => Uri
                                  ,node_started_at => Start
                                  ,server => self()
                                  },
                        spawn(fun() -> start_amqp(Broker) end),
                        {ok, starting};
                Error -> Error
            end;
        _ -> {error, duplicated_uri}
    end.

start_amqp(#{uri := Uri
            ,server := Server
            ,connection := Connection
            ,params := Params
            } = Broker0) ->
    Routines = [fun open_channel/1
               ,fun return_handler/1
               ,fun set_exchange/1
               ,fun declare_queue/1
               ,fun bind_queue/1
               ,fun consume_queue/1
               ],
    try
        Broker = lists:foldl(fun broker_fold/2, Broker0, Routines),
        Server ! {started, Broker}
    catch
        _E:Reason ->
            lager:error("error starting amqp : ~p", [{_E, Reason}]),
            catch(amqp_connection:close(Connection)),
            erlang:send_after(1500, Server, {reconnect, Uri, Params}),
            {error, Reason}
    end.

broker_fold(Fun, Broker) ->
    case Fun(Broker) of
       #{} = Updated -> Updated;
       _ -> Broker
    end.

broker_fold2(Fun, Broker) ->
    case catch(Fun(Broker)) of
       #{} = Updated -> Updated;
       _ -> Broker
    end.

stop_connections(Connections) ->
    maps:map(fun stop_broker/2, Connections).

stop_broker(_Uri, Broker) ->
    stop_amqp(Broker).

stop_amqp(Broker) ->
    Routines = [fun cancel_consume/1
               ,fun unregister_handler/1
               ,fun remove_monitors/1
               ,fun close_channel/1
               ,fun close_connection/1
               ],
    lists:foldl(fun broker_fold2/2, Broker, Routines).
    
cancel_consume(Broker = #{no_cancel := true}) ->
    maps:without([consumer_tag, no_cancel], Broker);
cancel_consume(Broker = #{channel := Channel
                         ,consumer_tag := ConsumerTag
                         }) ->
    #'basic.cancel_ok'{consumer_tag = ConsumerTag} =
        amqp_channel:call(Channel, #'basic.cancel'{consumer_tag = ConsumerTag}),
    maps:without([consumer_tag], Broker).

remove_monitors(Broker = #{channel_ref := ChannelRef
                          ,connection_ref := ConnectionRef
                          }) ->
    erlang:demonitor(ChannelRef),
    erlang:demonitor(ConnectionRef),
    maps:without([channel_ref,connection_ref], Broker).
    
unregister_handler(#{channel := Channel}) ->
    amqp_channel:unregister_return_handler(Channel).

close_connection(Broker = #{connection := Connection}) ->
    catch(amqp_connection:close(Connection)),
    maps:without([connection], Broker).

close_channel(Broker = #{channel := Channel}) ->
    catch(amqp_channel:close(Channel)),
    maps:without([channel], Broker).

-spec decode(kz_term:api_binary()) -> term().
decode('undefined') -> 'undefined';
decode(Bin) ->
    binary_to_term(base64:decode(Bin)).

publish(#{heartbeat := false}) -> 'ok';
publish(#{state := error}) ->
    lager:info("not publishing due to connection error");
publish(#{channel := Channel, queue := Q, exchange := X, node_started_at := Start}) ->
    Props = #'P_basic'{correlation_id = atom_to_binary(node(), utf8)
                      ,reply_to = Q
                      ,timestamp = os:system_time(microsecond)
                      ,headers = [{<<"distribution.ping">>, bool, true}
                                 ,{<<"node.start">>, timestamp, Start}
                                 ]
                      },
    Publish = #'basic.publish'{exchange = X},
    amqp_channel:call(Channel, Publish, #amqp_msg{props = Props}).

stop() ->
    gen_server:call(?MODULE, stop).

stop(Pid) ->
    gen_server:call(Pid, stop).

start_connections(#{env := Env}) ->
    URIs = proplists:get_value(connections, Env, []),
    lists:foreach(fun add_broker/1, URIs).

maybe_connect(0, {Node, RemoteStarted, LocalStarted}) ->
    maybe_connect(Node, RemoteStarted, LocalStarted);
maybe_connect(_, _) -> ok.
    
maybe_connect(Node, RemoteStarted, LocalStarted)
  when RemoteStarted < LocalStarted ->
  case lists:member(Node, erlang:nodes()) of
      true -> ok;
      false -> auto_connected(Node)
  end;
maybe_connect(Node, Started, Started) ->
    case Node < node()
        andalso not lists:member(Node, erlang:nodes())
    of
        true -> auto_connected(Node);
        false -> ok
    end;
maybe_connect(_Node, _RemoteStarted, _LocalStarted) ->
    ok.

auto_connected(Node) ->
    Fun = fun() ->
                  timer:sleep(2500),
                  net_kernel:connect_node(Node)
          end,
    spawn(Fun).


remove(Uri, #{refs := Refs, tags := Tags, pids := Pids} = State) ->
    #{connections := #{Uri := Broker} = Connections} = State,
    #{connection := Connection
     ,channel := Channel
     ,consumer_tag := Tag
     ,connection_ref := ConnectionRef
     ,channel_ref := ChannelRef
     ,params := Params
     } = Broker,
    catch(stop_amqp(Broker#{no_cancel => true})),
    erlang:send_after(5000, self(), {reconnect, Uri, Params}),
    State#{connections => maps:without([Uri], Connections)
          ,refs => maps:without([ConnectionRef, ChannelRef], Refs)
          ,tags => maps:without([Tag], Tags)
          ,pids => maps:without([Connection, Channel], Pids)
          }.
