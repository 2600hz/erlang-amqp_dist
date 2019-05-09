-module(amqp_dist_node).


-include("amqp_dist.hrl").

-behaviour(gen_server).

-export([start/4, start_link/3]).

-export([init/1, terminate/2, code_change/3, handle_call/3,
         handle_cast/2, handle_info/2]).


-export([send/2
        ,recv/3
        ,tick/1
        ,connected/2
        ,stats/1
        ,setup/2
        ,controller/2
        ,receiver/2
        ]).

%%--------------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------------

start_link(Connection, Node, Queue) ->
    gen_server:start_link(?MODULE, [Connection, Node, Queue], []).

start(Connection, Node, Queue, Action) ->
    case gen_server:start(?MODULE, [Connection, Node, Queue], []) of
        {ok, Pid} -> setup(Pid, Action);
        Error -> Error
    end.
            


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
init([Connection, Node, Queue]) ->
    process_flag(trap_exit, true),
    {ok, start_amqp(#{phase => init
                     ,remote_queue => Queue
                     ,node => Node
                     ,connection => Connection
                     ,exchange => <<>>
                     ,data => queue:new()
                     ,sent => 0
                     ,recv => 0
                     })}.

%% Closes the channel this gen_server instance started
%% @private
terminate(_Reason, #{channel := Channel, consumer_tag := ConsumerTag}) ->
    #'basic.cancel_ok'{consumer_tag = ConsumerTag} =
        amqp_channel:call(Channel, #'basic.cancel'{consumer_tag = ConsumerTag}),
    amqp_channel:unregister_return_handler(Channel),
    amqp_channel:close(Channel),
    ok.

%% Handle the application initiated stop by just stopping this gen server
%% @private
handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

handle_call(recv, _From, #{data := Data} = State) ->
    case queue:out(Data) of
        {{value, Value}, Q} -> {reply, {ok, Value}, State#{data => Q}};
        {empty, Q} -> {reply, {error, empty}, State#{data => Q}}
    end;

handle_call({send, Data}, _From, #{sent := Sent} = State) ->
    {Result, Size} = publish(Data, State),
    {reply, Result, State#{sent => Sent + Size}};

handle_call({connected, Receiver}, _From, State) ->
    {reply, ok, State#{receiver => Receiver
                      ,phase => connected
                      }};

handle_call(stats, _From, #{sent := Sent, recv := Received} = State) ->
    {reply, {ok, Received, Sent, false}, State};

handle_call({setup, connect}, From, State) ->
    publish({amqp_dist, connect}, State),
    {noreply, State#{caller => From}};

handle_call({setup, accept}, _From, State) ->
    publish({amqp_dist, confirmed}, State),
    {reply, {ok, self()}, State#{phase => handshake}};

handle_call({controller, Controller}, _From, State) ->    
    link(Controller),
    {reply, ok, State#{controller => Controller, controller_ref => erlang:monitor(process, Controller)}};

handle_call({receiver, Receiver}, _From, #{data := Queue} = State) ->
    send_pending(Receiver, queue:out(Queue)),
    {reply, ok, State#{receiver => Receiver, phase => connected, data := queue:new()}};

handle_call(_Msg, _From, State) ->
    {reply, ok, State}.

%% @private

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
    {noreply, State#{remote_queue => Queue
                    ,phase => handshake
                    }};

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
    {stop, normal, State};

handle_info({'EXIT', _Pid, _Reason}, State) ->
    {stop, normal, State};

handle_info(Msg, State) ->
    logger:info("unhandled info : ~p : ~p", [Msg, State]),
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
            lager:info("LDATA => ~p", [LData]),
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

setup(Pid, Action) ->
    gen_server:call(Pid, {setup, Action}).

controller(Pid, Controller) ->
    gen_server:call(Pid, {controller, Controller}).

receiver(Pid, Receiver) ->
    gen_server:call(Pid, {receiver, Receiver}).

%% supervisor(Pid, Supervisor) ->
%%     gen_server:call(Pid, {supervisor, Supervisor}).

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
    lists:foldl(fun start_amqp_fold/2, State, Routines).

start_amqp_fold(Fun, State) ->
    case Fun(State) of
       #{} = Map -> Map;
       _ -> State
    end.

open_channel(State = #{connection := Connection}) ->
    {ok, Channel} = amqp_connection:open_channel(
                        Connection, {amqp_direct_consumer, [self()]}),
    State#{channel => Channel}.

declare_queue(State = #{channel := Channel}) ->
    #'queue.declare_ok'{queue = Q} =
        amqp_channel:call(Channel, #'queue.declare'{exclusive   = true,
                                                    auto_delete = true}),
    State#{queue => Q}.

consume_queue(State = #{channel := Channel, queue := Q}) ->
    #'basic.consume_ok'{consumer_tag = ConsumerTag} =
        amqp_channel:call(Channel, #'basic.consume'{queue = Q, no_ack = true, no_local = true}),
    State#{consumer_tag => ConsumerTag}.

return_handler(#{channel := Channel}) ->
    amqp_channel:register_return_handler(Channel, self()).
