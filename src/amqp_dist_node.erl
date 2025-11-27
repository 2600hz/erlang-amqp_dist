-module(amqp_dist_node).


-include("amqp_dist.hrl").

-behaviour(gen_server).

-export([start/5]).

-export([init/1
        ,terminate/2
        ,code_change/3
        ,handle_call/3
        ,handle_cast/2
        ,handle_info/2
        ]).


-export([send/2
        ,recv/3
        ,tick/1
        ,connected/2
        ,stats/1
        ,setup/1
        ,controller/2
        ,receiver/2
        ,handshake_complete/2
        ]).

-define(PHASE_CHECK_INTERVAL, 20000).

%%--------------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------------

start(Connection, Label, Node, Queue, Action) ->
    case gen_server:start(?MODULE, [Connection, Label, Node, Queue, Action], []) of
        {ok, Pid} -> setup(Pid);
        Error -> Error
    end.
            

publish(_Data, #{heartbeat := false}) -> {0, 0};
publish(Data, #{channel := Channel, queue := Q, exchange := X, remote_queue := RK}) ->
    Props = #'P_basic'{correlation_id = atom_to_binary(node(), utf8), reply_to = Q, timestamp = os:system_time(microsecond)},
    Publish = #'basic.publish'{exchange = X, routing_key = RK, mandatory = true},
    Encoded = encode(Data),
    {amqp_channel:call(Channel, Publish, #amqp_msg{props = Props, payload = Encoded}), byte_size(Encoded)}.

%%--------------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------------

%% Sets up a reply queue and consumer within an existing channel
%% @private
init([Connection, Label, Node, Queue, Action]) ->
    process_flag(trap_exit, true),
    start_amqp(#{phase => init
                ,remote_queue => Queue
                ,node => Node
                ,connection => Connection
                ,connection_label => Label
                ,exchange => <<>>
                ,data => queue:new()
                ,sent => 0
                ,recv => 0
                ,action => Action
                }).

%% Closes the channel this gen_server instance started
%% @private
terminate(shutdown, State) ->
    log_termination(shutdown, State),
    ok;
terminate(killed, State) ->
    log_termination(killed, State),
    ok;
terminate(Reason, State) ->
    catch(stop_node(State)),
    log_termination(Reason, State),
    ok.

log_termination(normal, _State) -> ok;
log_termination(Reason, #{phase := Phase, action := Action, node := Node}) ->
    ?LOG_DEBUG("~s for node ~s terminated in ~s phase => ~p", [Action, Node, Phase, Reason]).

%% Handle the application initiated stop by just stopping this gen server
%% @private
handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

handle_call(recv, _From, #{data := Data} = State) ->
    case queue:out(Data) of
        {{value, Value}, Q} -> {reply, {ok, Value}, State#{data => Q}};
        {empty, Q} -> {reply, {error, empty}, State#{data => Q}}
    end;

handle_call({handshake_complete, Node}, _From, #{phase := handshake, node := Node} = State) ->
    ?LOG_INFO("handshake succeeded for node ~s", [Node]),
    {reply, ok, State};

handle_call({handshake_complete, HNode}, _From, #{phase := Phase, node := Node} = State) ->
    ?LOG_ERROR("handshake complete for node ~s with wrong local data ~p", [HNode, {Phase, Node}]),
    {reply, {error, wrong_state_or_pid}, State};

handle_call({send, Data}, _From, #{sent := Sent} = State) ->
    {Result, Size} = publish(Data, State),
    {reply, Result, State#{sent => Sent + Size}};

handle_call({connected, Receiver}, _From, State) ->
    {reply, ok, set_phase(State#{receiver => Receiver}, connected)};

handle_call(stats, _From, #{sent := Sent, recv := Received} = State) ->
    {reply, {ok, Received, Sent, false}, State};

handle_call({setup, connect}, From, State) ->
    publish({amqp_dist, connect}, State),
    {noreply, State#{caller => From}};

handle_call({setup, accept}, _From, State) ->
    publish({amqp_dist, confirmed}, State),
    {reply, {ok, self()}, set_phase(State, handshake)};

handle_call(setup, From, #{action := connect} = State) ->
    publish({amqp_dist, connect}, State),
    {noreply, State#{caller => From}};

handle_call(setup, _From, #{action := accept} = State) ->
    publish({amqp_dist, confirmed}, State),
    {reply, {ok, self()}, set_phase(State, handshake)};

handle_call({controller, Controller}, _From, State) ->    
    link(Controller),
    {reply, ok, State#{controller => Controller, controller_ref => erlang:monitor(process, Controller)}};

handle_call({receiver, Receiver}, _From, #{data := Queue} = State) ->
    send_pending(Receiver, queue:out(Queue)),
    {reply, ok, set_phase(State#{receiver => Receiver, receiver_ref => erlang:monitor(process, Receiver), data := queue:new()}, connected)};

handle_call(_Msg, _From, State) ->
    {reply, ok, State}.

%% @private

handle_cast(tick, #{phase := handshake, node := Node} = State) ->
    ?LOG_INFO("connected to ~s", [Node]),
    handle_cast(tick, State#{phase => connected});

handle_cast(tick, #{sent := Sent} = State) ->
    {_Result, Size} = publish(keep_alive, State),
    {noreply, State#{sent => Sent + Size}};

%% handle_cast(tick, #{sent := Sent, recv := Received} = State) ->
%%     {noreply, State#{sent => Sent + 1, recv => Received + 1}};

handle_cast(_Msg, State) ->
    {noreply, State}.

%% @private
handle_info({#'basic.consume'{}, _Pid}, State) ->
    {noreply, State};

handle_info({#'basic.return'{}, _}, #{receiver := Receiver} = State) ->
    Receiver ! {amqp_closed, self()},
    {noreply, State};

handle_info({#'basic.return'{}, _}, State) ->
    {noreply, State};

%% @private
handle_info(#'basic.consume_ok'{}, State) ->
    {noreply, State};

%% @private
handle_info(#'basic.cancel'{}, State) ->
    {noreply, State};

%% @private
handle_info(#'basic.cancel_ok'{}, State) ->
    {stop, normal, State};

handle_info({#'basic.deliver'{}
            ,#amqp_msg{props = #'P_basic'{reply_to = Queue}
                      ,payload = Payload
                      }
            }
            ,State = #{phase := init
                      ,caller := Pid
                      }) ->
    gen_server:reply(Pid, {ok, self()}),
    {amqp_dist, confirmed} = decode(Payload),
    {noreply, set_phase(State#{remote_queue => Queue}, handshake)};

handle_info({#'basic.deliver'{}
            ,#amqp_msg{props = #'P_basic'{reply_to = Queue}
                      ,payload = Payload
                      }
            }
            ,State = #{phase := handshake
                      ,remote_queue := Queue
                      ,data := QData
                      ,recv := Recv
                      }) ->
    Data = decode(Payload),
    {noreply, State#{recv => Recv + byte_size(Payload)
                    ,data => queue:in(Data, QData)
                    }};

handle_info({#'basic.deliver'{}
            ,#amqp_msg{props = #'P_basic'{reply_to = Queue}
                      ,payload = Payload
                      }
            }
            ,State = #{phase := connected
                      ,remote_queue := Queue
                      ,receiver := Receiver
                      ,recv := Recv
                      }) ->
    case decode(Payload) of
        keep_alive -> ok;
        Data -> Receiver ! {data, self(), Data}
    end,
    {noreply, State#{recv => Recv + byte_size(Payload)}};

handle_info({'DOWN', ControllerRef, process, Controller, _Info}
           ,#{controller := Controller, controller_ref := ControllerRef} = State) ->
    ?LOG_INFO("controller ~p went down => ~p", [Controller, _Info]),
    {stop, normal, State};

handle_info({'DOWN', ReceiverRef, process, Receiver, _Info}
           ,#{receiver := Receiver, receiver_ref := ReceiverRef} = State) ->
    ?LOG_INFO("receiver ~p went down => ~p", [Receiver, _Info]),
    {stop, normal, State};

handle_info({'DOWN', ChannelRef, process, Channel, _Info}
           ,#{channel := Channel, channel_ref := ChannelRef} = State) ->
    ?LOG_WARNING("channel ~p went down => ~p", [Channel, _Info]),
    {noreply, State#{heartbeat => false}};

handle_info({'DOWN', ConnectionRef, process, Connection, _Info}
           ,#{connection := Connection, connection_ref := ConnectionRef} = State) ->
    ?LOG_WARNING("connection ~p went down => ~p", [Connection, _Info]),
    {stop, normal, State};

handle_info({'DOWN', _Ref, process, _Pid, 'shutdown'}, State) ->
    ?LOG_INFO("pid ~p was shutdown => ~p", [_Pid, State]),
    {noreply, State#{heartbeat => false}};

handle_info({'DOWN', _Ref, process, _Pid, 'killed'}, State) ->
    ?LOG_INFO("pid ~p was killed", [_Pid]),
    {noreply, State#{heartbeat => false}};

handle_info({'DOWN', _Ref, process, _Pid, _Info}, State) ->
    ?LOG_INFO("pid ~p went down => ~p", [_Pid, _Info]),
    {noreply, State#{heartbeat => false}};

handle_info({'EXIT', _Pid, 'shutdown'}, State) ->
    ?LOG_INFO("pid ~p exit with shutdown", [_Pid]),
    {stop, normal, State};

handle_info({'EXIT', _Pid, 'killed'}, State) ->
    ?LOG_INFO("pid ~p exit with killed", [_Pid]),
    {stop, normal, State};

handle_info({'EXIT', _Pid, _Reason}, State) ->
    ?LOG_INFO("pid ~p exit with reason => ~p", [_Pid, _Reason]),
    {stop, normal, State};

handle_info({terminate_if_phase, Phase}, #{phase := Phase, action := Action, node := Node} = State) ->
    ?LOG_INFO("terminate ~s on stalled ~s phase for node ~s", [Action, Phase, Node]),
    {stop, normal, State};

handle_info({terminate_if_phase, _Phase}, State) ->
    {noreply, State};

handle_info({handshake_complete, Node}, #{phase := handshake, node := Node} = State) ->
    ?LOG_INFO("handshake succeeded for node ~s", [Node]),
    {noreply, State};

handle_info(Msg, State) ->
    ?LOG_INFO("unhandled info : ~p : ~p", [Msg, State]),
    {noreply, State}.


%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


-spec encode(term()) -> kz_term:ne_binary().
encode(Term) ->
    base64:encode(term_to_binary(Term)).

-spec decode(binary()) -> term().
decode(Bin) ->
    binary_to_term(base64:decode(Bin)).

recv(Pid, Length, Timeout) ->
    recv(Pid, Length, 0, [], {erlang:system_time(millisecond), Timeout}).

recv(_Pid, 0, Collected, Acc, _Timeout)
  when Collected > 0 ->
    {ok, Acc};
recv(_Pid, Length, Length, Acc, _Timeout)
  when Length > 0 ->
    {ok, Acc};
recv(_Pid, _Length, _Collected, _Acc, {_, Timeout})
  when is_integer(Timeout) andalso Timeout =< 0 ->
    {error, timeout};
recv(Pid, Length, Collected, Acc, Timeout) ->
    case gen_server:call(Pid, recv, 'infinity') of
        {ok, Data} when is_binary(Data) ->
            LData = Acc ++ binary_to_list(Data),
            recv(Pid, Length, length(LData), LData, decr_timeout(Timeout));
       {ok, Data} when is_list(Data) ->
            LData = Acc ++ Data,
            recv(Pid, Length, length(LData), LData, decr_timeout(Timeout));
        {error, empty} ->
            timer:sleep(25),
            recv(Pid, Length, Collected, Acc, decr_timeout(Timeout))
    end.

decr_timeout({_Start, 'infinity'}) ->
    Now = erlang:system_time(millisecond),
    {Now, 'infinity'};
decr_timeout({Start, Timeout}) ->
    Now = erlang:system_time(millisecond),
    {Now, Timeout - (Now - Start)}.

send(Pid, Data) ->
    gen_server:call(Pid, {send, Data}).

tick(Pid) ->
    gen_server:cast(Pid, tick).

connected(Pid, Receiver) ->
    gen_server:call(Pid, {connected, Receiver}).

stats(Pid) ->
    gen_server:call(Pid, stats).

setup(Pid) ->
    gen_server:call(Pid, setup).

controller(Pid, Controller) ->
    gen_server:call(Pid, {controller, Controller}).

receiver(Pid, Receiver) ->
    gen_server:call(Pid, {receiver, Receiver}).

send_pending(Receiver, {{value, Value}, Queue}) ->
     Receiver ! {data, self(), Value},
     send_pending(Receiver, queue:out(Queue));
send_pending(_Receiver, {empty, _Queue}) -> ok.

start_amqp(State) ->
    Routines = [fun open_channel/1
               ,fun return_handler/1
               ,fun declare_queue/1
               ,fun consume_queue/1
               ],
    try
        {ok, lists:foldl(fun start_amqp_fold/2, State, Routines)}
    catch
        Class:Exception:Stacktrace ->
            ?LOG_ALERT("error starting dist node => ~p", [{Class,Exception,Stacktrace}]),
            {error, Exception}
    end.

start_amqp_fold(Fun, State) ->
    case Fun(State) of
       #{} = Map -> Map;
       _ -> State
    end.

open_channel(State = #{connection := Connection}) ->
    {ok, Channel} = amqp_connection:open_channel(Connection, {amqp_direct_consumer, [self()]}),
    State#{channel => Channel
          ,channel_ref => erlang:monitor(process, Channel)
          ,connection_ref => erlang:monitor(process, Connection)
          }.

queue_declare_cmd(Broker) ->
    #'queue.declare'{exclusive = true
                    ,auto_delete = true
                    ,queue = queue_name(Broker)
                    }.

queue_name(#{connection_label := undefined, node := Node, action := Action}) ->
    list_to_binary(["amqp_dist_node-", atom_to_list(node()), "-", atom_to_list(Action), "-", atom_to_list(Node), "-", pid_to_list(self())]);
queue_name(#{connection_label := Label, node := Node, action := Action}) ->
    list_to_binary(["amqp_dist_node-", atom_to_list(Label), "-", atom_to_list(node()), "-", atom_to_list(Action), "-", atom_to_list(Node), "-", pid_to_list(self())]).

declare_queue(State = #{channel := Channel}) ->
    #'queue.declare_ok'{queue = Q} = amqp_channel:call(Channel, queue_declare_cmd(State)),
    State#{queue => Q}.

consume_queue(State = #{channel := Channel, queue := Q}) ->
    #'basic.consume_ok'{consumer_tag = ConsumerTag} =
        amqp_channel:call(Channel, #'basic.consume'{queue = Q, no_ack = true, no_local = true}),
    State#{consumer_tag => ConsumerTag}.

return_handler(#{channel := Channel}) ->
    amqp_channel:register_return_handler(Channel, self()).

stop_node(#{channel := Channel, consumer_tag := ConsumerTag}) ->
    #'basic.cancel_ok'{consumer_tag = ConsumerTag} =
        amqp_channel:call(Channel, #'basic.cancel'{consumer_tag = ConsumerTag}),
    amqp_channel:unregister_return_handler(Channel),
    catch(amqp_channel:close(Channel)).

set_phase(State, handshake = Phase) ->
    Ref = erlang:send_after(?PHASE_CHECK_INTERVAL, self(), {terminate_if_phase, handshake}),
    State#{phase => Phase, check_phase_timer_ref => Ref};
set_phase(State, Phase) ->
    State#{phase => Phase}.

handshake_complete(Pid, Node) ->
    gen_server:call(Pid, {handshake_complete, Node}).
