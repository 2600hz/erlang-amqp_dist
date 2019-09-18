-module(amqp_dist).

-include("amqp_dist.hrl").

%-define(dist_util,amqp_dist_util).

-compile({no_auto_import,[nodes/0]}).

%%
%% This is an example of how to plug in an arbitrary distribution
%% carrier for Erlang using distribution processes.
%%
%% This module is based on gen_tcp_dist using amqp as the distribution protocol
%%

-export([listen/1
        ,accept/1
        ,accept_connection/5
        ,setup/5
        ,close/1
        ,select/1
        ]).

%% api

-export([add_broker/1, add_brokers/1]).
-export([nodes/0]).
-export([is_up/1]). 
  
%% internal exports

-export([dist_cntrlr_setup/1
        ,dist_cntrlr_input_setup/3
        ,dist_cntrlr_tick_handler/1
        ]).

-export([start_accept/2
        ,accept_loop/2
        ,do_accept/6
        ,do_setup/6
        ]).

-import(error_logger,[error_msg/2]).

-record(fake_socket, {read = 0,
                      write = 0,
                      pending = 0,
                      pid = self() :: pid(),
                      name :: term(),
                      mypid :: pid()
                     }).

%% API

add_brokers(Uris) ->
    [add_broker(Uri)|| Uri <- Uris],
    'ok'.

add_broker(Uri) ->
    amqp_dist_acceptor:add_broker(Uri).

nodes() ->
    amqp_dist_acceptor:nodes().

is_up(Node) ->
    amqp_dist_acceptor:is_up(Node).

%% ------------------------------------------------------------
%%  Select this protocol based on node name
%%  select(Node) => Bool
%% ------------------------------------------------------------
select(Node) ->
    amqp_dist_acceptor:is_up(Node).

%% ------------------------------------------------------------
%% Create the listen socket, i.e. the port that this erlang
%% node is accessible through.
%% ------------------------------------------------------------
listen(Name) ->
    case create_acceptor(Name) of
        {ok, Pid} ->
            {ok, {#fake_socket{name=Name, mypid=Pid}
                 ,#net_address{address = []
                              ,host = inet:gethostname()
                              ,protocol = amqp
                              ,family = amqp
                              }
                 ,3
                 }
            };
        Else -> Else
    end.

create_acceptor(Name) ->
    application:load(amqp_dist),
    amqp_dist_acceptor:start(self(), Name).


%% ------------------------------------------------------------
%% Accepts new connection attempts from other Erlang nodes.
%% ------------------------------------------------------------
accept(Listen) ->
    spawn_opt(?MODULE, start_accept, [self(), Listen], [link, {priority, max}]).

start_accept(Kernel, Listen) ->
    amqp_dist_acceptor:acceptor(self()),
    accept_loop(Kernel, Listen).

accept_loop(Kernel, Listen) ->
    receive
        {connection, Node, Connection, Queue} ->
            Kernel ! {accept, self(), {Node, Connection, Queue, Listen}, amqp, amqp},
            receive
                {Kernel, controller, SupervisorPid} ->
                    SupervisorPid ! {self(), controller};
                {Kernel, unsupported_protocol} ->
                    exit(unsupported_protocol)
            end,
            accept_loop(Kernel, Listen)
    end.

%% ------------------------------------------------------------
%% Accepts a new connection attempt from another Erlang node.
%% Performs the handshake with the other side.
%% ------------------------------------------------------------
accept_connection(AcceptPid, Params, MyNode, Allowed, SetupTime) ->
    spawn_opt(?MODULE, do_accept,
          [self(), AcceptPid, Params, MyNode, Allowed, SetupTime],
          [link, {priority, max}]).

do_accept(Kernel, AcceptPid, {Node, Connection, Queue, _Listen}, MyNode, Allowed, SetupTime) ->
    {ok, Pid} = amqp_dist_acceptor:accept({Node, Connection, Queue}),
    DistCtrl = spawn_dist_cntrlr(Pid),
    call_ctrlr(DistCtrl, {supervisor, self()}),
    amqp_dist_node:controller(Pid, self()),
    receive
    {AcceptPid, controller} ->
        Timer = dist_util:start_timer(SetupTime),
        HSData0 = hs_data_common(DistCtrl),
        HSData = HSData0#hs_data{kernel_pid = Kernel
                                ,this_node = MyNode
                                ,socket = DistCtrl
                                ,timer = Timer
                                ,this_flags = 0
                                ,allowed = Allowed
                                },
        dist_util:handshake_other_started(HSData)
    end.

%% ------------------------------------------------------------
%% Setup a new connection to another Erlang node.
%% Performs the handshake with the other side.
%% ------------------------------------------------------------
setup(Node, Type, MyNode, LongOrShortNames,SetupTime) ->
    spawn_opt(?MODULE, do_setup, 
          [self(), Node, Type, MyNode, LongOrShortNames, SetupTime],
          [link, {priority, max}]).

do_setup(Kernel, Node, Type, MyNode, _LongOrShortNames, SetupTime) ->
    Timer = dist_util:start_timer(SetupTime),
    case amqp_dist_acceptor:connect(Node) of
        {'ok', Pid} ->
            amqp_dist_node:controller(Pid, self()),
            dist_util:reset_timer(Timer),
            DistCtrl = spawn_dist_cntrlr(Pid),
            call_ctrlr(DistCtrl, {supervisor, self()}),
            HSData0 = hs_data_common(DistCtrl),
            HSData = HSData0#hs_data{kernel_pid = Kernel
                                    ,other_node = Node
                                    ,this_node = MyNode
                                    ,socket = DistCtrl
                                    ,timer = Timer
                                    ,this_flags = 0
                                    ,other_version = 5
                                    ,request_type = Type
                                    },
            dist_util:handshake_we_started(HSData);
        {error, _Reason} ->
            ?shutdown(Node)
    end.

%%
%% Close a socket.
%%
close(Listen) ->
    lager:info("CLOSE ~p", [Listen]).


split_node([Chr|T], Chr, Ack) -> [lists:reverse(Ack)|split_node(T, Chr, [])];
split_node([H|T], Chr, Ack)   -> split_node(T, Chr, [H|Ack]);
split_node([], _, Ack)        -> [lists:reverse(Ack)].

hs_data_common(DistCtrl) ->
    TickHandler = call_ctrlr(DistCtrl, tick_handler),
    Pid = call_ctrlr(DistCtrl, pid),
    #hs_data{f_send = send_fun(),
             f_recv = recv_fun(),
             f_setopts_pre_nodeup = setopts_pre_nodeup_fun(),
             f_setopts_post_nodeup = setopts_post_nodeup_fun(),
             f_getll = getll_fun(),
             f_handshake_complete = handshake_complete_fun(),
             f_address = address_fun(),
             mf_setopts = setopts_fun(DistCtrl, Pid),
             mf_getopts = getopts_fun(DistCtrl, Pid),
             mf_getstat = getstat_fun(DistCtrl, Pid),
             mf_tick = tick_fun(DistCtrl, TickHandler)}.

%%% ------------------------------------------------------------
%%% Distribution controller processes
%%% ------------------------------------------------------------

%% In order to avoid issues with lingering signal binaries
%% we enable off-heap message queue data as well as fullsweep
%% after 0. The fullsweeps will be cheap since we have more
%% or less no live data.
-define(DIST_CNTRL_COMMON_SPAWN_OPTS,
        [{message_queue_data, off_heap},
         {fullsweep_after, 0}]).

tick_fun(DistCtrl, TickHandler) ->
    fun (Ctrl) when Ctrl == DistCtrl ->
            TickHandler ! tick
    end.

getstat_fun(DistCtrl, Pid) ->
    fun (Ctrl) when Ctrl == DistCtrl ->
            amqp_dist_node:stats(Pid)
    end.

setopts_fun(DistCtrl, Socket) ->
    fun (Ctrl, Opts) when Ctrl == DistCtrl ->
            setopts(Socket, Opts)
    end.

getopts_fun(DistCtrl, Socket) ->
    fun (Ctrl, Opts) when Ctrl == DistCtrl ->
            getopts(Socket, Opts)
    end.

setopts(_S, Opts) ->
    case [Opt || {K,_}=Opt <- Opts,
         K =:= active orelse K =:= deliver orelse K =:= packet] of
    [] -> 'ok'; %%inet:setopts(S,Opts);
    Opts1 -> {error, {badopts,Opts1}}
    end.

getopts(_S, _Opts) ->
    [].

send_fun() ->
    fun (Ctrlr, Packet) ->
             call_ctrlr(Ctrlr, {send, Packet})
    end.

recv_fun() ->
    fun (Ctrlr, Length, Timeout) ->
           call_ctrlr(Ctrlr, {recv, Length, Timeout})
    end.

getll_fun() ->
    fun (Ctrlr) ->
            call_ctrlr(Ctrlr, getll)
    end.

address_fun() ->
    fun (Ctrlr, Node) ->
            case call_ctrlr(Ctrlr, {address, Node}) of
                {error, no_node} -> ?shutdown(no_node);
                Res -> Res
            end
    end.

get_remote_id(_Socket, Node) ->
    [_, Host] = split_node(atom_to_list(Node), $@, []),
    #net_address {
       address = [],
       host = Host,
       protocol = amqp,
       family = amqp
                 }.

setopts_pre_nodeup_fun() ->
    fun (Ctrlr) ->
            call_ctrlr(Ctrlr, pre_nodeup)
    end.

setopts_post_nodeup_fun() ->
    fun (Ctrlr) ->
            call_ctrlr(Ctrlr, post_nodeup)
    end.

handshake_complete_fun() ->
    fun (Ctrlr, Node, DHandle) ->
            call_ctrlr(Ctrlr, {handshake_complete, Node, DHandle})
    end.

call_ctrlr(Ctrlr, Msg) ->
    Ref = erlang:monitor(process, Ctrlr),
    Ctrlr ! {Ref, self(), Msg},
    receive
        {Ref, Res} ->
            erlang:demonitor(Ref, [flush]),
            Res;
        {'DOWN', Ref, process, Ctrlr, Reason} ->
            exit({dist_controller_exit, Reason})
    end.

dist_cntrlr_tick_handler(Pid) ->
    receive
        tick ->
            amqp_dist_node:tick(Pid);
        _ ->
            ok
    end,
    dist_cntrlr_tick_handler(Pid).

spawn_dist_cntrlr(Pid) ->
    spawn_opt(?MODULE, dist_cntrlr_setup, [Pid],
              [{priority, max}] ++ ?DIST_CNTRL_COMMON_SPAWN_OPTS).

dist_cntrlr_setup(Pid) ->
    TickHandler = spawn_opt(?MODULE, dist_cntrlr_tick_handler,
                            [Pid],
                            [link, {priority, max}] 
                            ++ ?DIST_CNTRL_COMMON_SPAWN_OPTS),
    dist_cntrlr_setup_loop(Pid, TickHandler, undefined).

%%
%% During the handshake phase we loop in dist_cntrlr_setup().
%% When the connection is up we spawn an input handler and
%% continue as output handler.
%%
dist_cntrlr_setup_loop(Pid, TickHandler, Sup) ->
    receive
        {amqp_closed, Pid} ->
            exit(connection_closed);

        {Ref, From, {supervisor, SupervisorPid}} ->
            Res = link(SupervisorPid),
            From ! {Ref, Res},
            dist_cntrlr_setup_loop(Pid, TickHandler, SupervisorPid);

        {Ref, From, tick_handler} ->
            From ! {Ref, TickHandler},
            dist_cntrlr_setup_loop(Pid, TickHandler, Sup);

        {Ref, From, pid} ->
            From ! {Ref, Pid},
            dist_cntrlr_setup_loop(Pid, TickHandler, Sup);

        {Ref, From, {send, Packet}} ->
            Res = amqp_dist_node:send(Pid, list_to_binary(Packet)),
            From ! {Ref, Res},
            dist_cntrlr_setup_loop(Pid, TickHandler, Sup);

        {Ref, From, {recv, Length, Timeout}} ->
            Res = amqp_dist_node:recv(Pid, Length, Timeout),
            From ! {Ref, Res},
            dist_cntrlr_setup_loop(Pid, TickHandler, Sup);

        {Ref, From, getll} ->
            From ! {Ref, {ok, self()}},
            dist_cntrlr_setup_loop(Pid, TickHandler, Sup);

        {Ref, From, {address, Node}} ->
            ID = get_remote_id(Pid, Node),
            From ! {Ref, {ok, ID}},
            dist_cntrlr_setup_loop(Pid, TickHandler, Sup);

        {Ref, From, pre_nodeup} ->
            From ! {Ref, ok},
            dist_cntrlr_setup_loop(Pid, TickHandler, Sup);

        {Ref, From, post_nodeup} ->
            From ! {Ref, ok},
            dist_cntrlr_setup_loop(Pid, TickHandler, Sup);

        {Ref, From, {handshake_complete, _Node, DHandle}} ->
            From ! {Ref, ok},
            %% Handshake complete! Begin dispatching traffic...

            %% We use separate process for dispatching input. This
            %% is not necessary, but it enables parallel execution
            %% of independent work loads at the same time as it
            %% simplifies the the implementation...
            InputHandler = spawn_opt(?MODULE, dist_cntrlr_input_setup,
                                     [DHandle, Pid, Sup],
                                     [link] ++ ?DIST_CNTRL_COMMON_SPAWN_OPTS),

            ok = erlang:dist_ctrl_input_handler(DHandle, InputHandler),

            InputHandler ! DHandle,

            %% From now on we execute on normal priority
            process_flag(priority, normal),
            erlang:dist_ctrl_get_data_notification(DHandle),
            dist_cntrlr_output_loop(DHandle, Pid);
        _Other ->
            dist_cntrlr_setup_loop(Pid, TickHandler, Sup)
    end.

dist_cntrlr_input_setup(DHandle, Pid, _Sup) ->
    %% Ensure we don't try to put data before registerd
    %% as input handler...
    receive
        DHandle ->
            amqp_dist_node:receiver(Pid, self()),
            dist_cntrlr_input_loop(DHandle, Pid)
    end.

dist_cntrlr_input_loop(DHandle, Pid) ->
    receive
        {amqp_closed, Pid} ->
            %% Connection to remote node terminated...
            exit(connection_closed);

        {tcp_closed, Pid} ->
            %% Connection to remote node terminated...
            exit(connection_closed);
        
        {data, Pid, Data} ->
            %% Incoming data from remote node...
            try erlang:dist_ctrl_put_data(DHandle, Data)
            catch _ : _ -> death_row()
            end,
            dist_cntrlr_input_loop(DHandle, Pid);

        _Other ->
            %% Ignore...
            dist_cntrlr_input_loop(DHandle, Pid)
    end.

dist_cntrlr_send_data(DHandle, Pid) ->
    case erlang:dist_ctrl_get_data(DHandle) of
        none ->
            erlang:dist_ctrl_get_data_notification(DHandle);
        Data ->
            amqp_dist_node:send(Pid, Data),
            dist_cntrlr_send_data(DHandle, Pid)
    end.


dist_cntrlr_output_loop(DHandle, Pid) ->
    receive
        dist_data ->
            %% Outgoing data from this node...
            try dist_cntrlr_send_data(DHandle, Pid)
            catch _ : _ -> death_row()
            end,
            dist_cntrlr_output_loop(DHandle, Pid);

        {send, From, Ref, Data} ->
            %% This is for testing only!
            %%
            %% Needed by some OTP distribution
            %% test suites...
            amqp_dist_node:send(Pid, Data),
            From ! {Ref, ok},
            dist_cntrlr_output_loop(DHandle, Pid);

        _Other ->
            %% Drop garbage message...
            dist_cntrlr_output_loop(DHandle, Pid)

    end.

death_row() ->
    death_row(connection_closed).

death_row(normal) ->
    %% We do not want to exit with normal
    %% exit reason since it wont bring down
    %% linked processes...
    death_row();
death_row(Reason) ->
    %% When the connection is on its way down operations
    %% begin to fail. We catch the failures and call
    %% this function waiting for termination. We should
    %% be terminated by one of our links to the other
    %% involved parties that began bringing the
    %% connection down. By waiting for termination we
    %% avoid altering the exit reason for the connection
    %% teardown. We however limit the wait to 5 seconds
    %% and bring down the connection ourselves if not
    %% terminated...
    receive after 5000 -> exit(Reason) end.
