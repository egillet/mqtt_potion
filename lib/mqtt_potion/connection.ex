defmodule MqttPotion.Connection do
  @moduledoc """
  MQTT Connection
  """

  use GenServer

  require Logger

  alias MqttPotion.Message

  @type opts :: MqttPotion.opts()
  @type pub_opts :: MqttPotion.pub_opts()
  @type subscription :: MqttPotion.subscription()

  defmodule State do
    @moduledoc false
    @type pub_opts :: MqttPotion.pub_opts()
    @type t :: %__MODULE__{
            conn_pid: pid(),
            username: String.t(),
            client_id: String.t(),
            handler_pid: pid(),
            opts: MqttPotion.opts(),
            reconnect: {delay :: non_neg_integer, max_delay :: non_neg_integer},
            subscriptions: [MqttPotion.subscription()],
            outgoing: [{topic :: String.t(), message :: String.t(), opts :: pub_opts()}]
          }
    defstruct [
      :conn_pid,
      :username,
      :client_id,
      :handler_pid,
      :opts,
      :reconnect,
      :subscriptions,
      :outgoing
    ]
  end

  @opt_keys [
    :owner,
    :handler_pid,
    :host,
    :hosts,
    :port,
    :tcp_opts,
    :ssl,
    :ssl_opts,
    :ws_path,
    :connect_timeout,
    :bridge_mode,
    :client_id,
    :clean_start,
    :username,
    :password,
    :protocol_version,
    :keepalive,
    :max_inflight,
    :retry_interval,
    :will_topic,
    :will_payload,
    :will_retain,
    :will_qos,
    :will_props,
    :auto_ack,
    :ack_timeout,
    :force_ping,
    :opts,
    :properties,
    :reconnect,
    :subscriptions,
    :start_when
  ]

  @doc """
  Start the MQTT Client GenServer.
  """
  @spec start_link(opts) :: {:ok, pid}
  def start_link(opts) do
    {name, opts} = Keyword.pop(opts, :name, nil)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  # ----------------------------------------------------------------------------
  # Callbacks
  # ----------------------------------------------------------------------------

  ## Init

  @impl GenServer
  def init(opts) do
    Process.flag(:trap_exit, true)

    opts = take_opts(opts)
    {{delay, max_delay}, opts} = Keyword.pop(opts, :reconnect, {500, 60_000})
    {start_when, opts} = Keyword.pop(opts, :start_when, :now)
    {subscriptions, opts} = Keyword.pop(opts, :subscriptions, [])

    handler_pid = opts[:handler_pid]

    ssl_opts =
      opts
      |> Keyword.get(:ssl_opts, [])
      |> Keyword.put_new(:server_name_indication, String.to_charlist(opts[:host]))

    opts = Keyword.put(opts, :ssl_opts, ssl_opts)

    # EMQTT `msg_handler` functions
    handler_functions = %{
      puback: &handle_puback(&1, handler_pid),
      publish: &handle_publish(&1, handler_pid),
      disconnected: &handle_disconnect(&1, handler_pid)
    }

    state = %State{
      client_id: opts[:client_id],
      reconnect: {delay, max_delay},
      subscriptions: subscriptions,
      username: opts[:username],
      handler_pid: handler_pid,
      opts: [{:msg_handler, handler_functions} | opts],
      outgoing: []
    }

    {:ok, state, {:continue, {:start_when, start_when}}}
  end

  ## Continue

  @impl GenServer

  def handle_continue({:start_when, :now}, state) do
    {:noreply, state, {:continue, {:connect, 0}}}
  end

  def handle_continue({:start_when, start_when}, state) do
    {{module, function, args}, retry_in} = start_when

    if apply(module, function, args) do
      {:noreply, state, {:continue, {:connect, 0}}}
    else
      Process.sleep(retry_in)
      {:noreply, state, {:continue, {:start_when, start_when}}}
    end
  end

  def handle_continue({:connect, attempt}, state) do
    case connect(state) do
      {:ok, state} ->
        {:noreply, state}

      {:error, reason} ->
        %{reconnect: {initial_delay, max_delay}} = state
        delay = retry_delay(initial_delay, max_delay, attempt)

        Logger.debug(
          "[MqttPotion] Unable to connect: #{inspect(reason)}, retrying in #{delay} ms"
        )

        if attempt <= 2 do
          # if few attempts, continue retrying and delay connections
          :timer.sleep(delay)
          {:noreply, state, {:continue, {:connect, attempt + 1}}}
        else
          # ... else let clients see errors.
          Process.send_after(self(), {:reconnect, attempt + 1}, delay)
          {:noreply, state, {:continue}}
        end
    end
  end

  ## Call

  @impl GenServer

  def handle_call({:publish, topic, message, opts}, _from, state) do
    case pub_or_queue(state, topic, message, opts) do
      :ok ->
        {:reply, :ok, state}

      {:queued, state} ->
        {:reply, :ok, state}

      {:error, _} = error ->
        {:reply, error, state}
    end
  end

  def handle_call({:subscribe, subscription}, _from, state) do
    case sub_or_nop(state, subscription) do
      :ok ->
        {:reply, :ok, state}

      {:error, _} = error ->
        {:reply, error, state}
    end
  end

  def handle_call({:unsubscribe, topic}, _from, state) do
    case unsub_or_nop(state, topic) do
      :ok ->
        {:reply, :ok, state}

      {:error, _} = error ->
        {:reply, error, state}
    end
  end

  def handle_call(:disconnect, _from, state) do
    :ok = dc(state)
    {:reply, :ok, state}
  end

  ## Cast

  @impl GenServer

  def handle_cast({:publish, topic, message, opts}, state) do
    case pub_or_queue(state, topic, message, opts) do
      :ok ->
        {:noreply, state}

      {:queued, state} ->
        {:noreply, state}

      {:error, _} = error ->
        log_error(error, "publish error")
        {:noreply, state}
    end
  end

  def handle_cast({:subscribe, subscription}, state) do
    case sub_or_nop(state, subscription) do
      :ok ->
        {:noreply, state}

      {:error, _} = error ->
        log_error(error, "subscribe error")
        {:noreply, state}
    end
  end

  def handle_cast({:unsubscribe, topic}, state) do
    case unsub_or_nop(state, topic) do
      :ok ->
        {:noreply, state}

      {:error, _} = error ->
        log_error(error, "unsubscribe error")
        {:noreply, state}
    end
  end

  def handle_cast(:disconnect, state) do
    :ok = dc(state)
    {:noreply, state}
  end

  ## Info

  @impl GenServer

  def handle_info({:reconnect, attempt}, %{reconnect: {initial_delay, max_delay}} = state) do
    Logger.debug("[MqttPotion] Trying to reconnect")

    case connect(state) do
      {:ok, state} ->
        {:noreply, state}

      {:error, reason} ->
        delay = retry_delay(initial_delay, max_delay, attempt)

        Logger.debug(
          "[MqttPotion] Unable to reconnect: #{inspect(reason)}, retrying in #{delay} ms"
        )

        Process.send_after(self(), {:reconnect, attempt + 1}, delay)
        {:noreply, state}
    end
  end

  def handle_info({:EXIT, pid, _}, state) do
    state =
      if pid == state.conn_pid do
        Logger.warn("[MqttPotion] Got Exit")
        Process.send_after(self(), {:reconnect, 0}, 0)
        %{state | conn_pid: nil}
      else
        state
      end

    {:noreply, state}
  end

  def handle_info(msg, state) do
    Logger.warn("[MqttPotion] Unhandled message #{inspect(msg)}")
    {:noreply, state}
  end

  ## Disconnect

  def handle_disconnect({reason_code, properties}, handler_pid) do
    Logger.warn(
      "[MqttPotion] Disconnect received: reason #{inspect(reason_code)}, properties: #{inspect(properties)}"
    )

    GenServer.cast(handler_pid, {:mqtt, :disconnect, reason_code, properties})
    :ok
  rescue
    e ->
      Logger.error("Got error in handle_disconnect: #{inspect(e)}")
      :ok
  end

  ## PubAck

  def handle_puback(ack, handler_pid) do
    Logger.debug("[MqttPotion] PUBACK received #{inspect(ack)}")
    GenServer.cast(handler_pid, {:mqtt, :puback})

    :ok
  rescue
    e ->
      Logger.error("Got error in handle_puback: #{inspect(e)}")
      :ok
  end

  ## Publish

  def handle_publish(message, handler_pid) do
    Logger.debug("[MqttPotion] Publish: #{inspect(message)}")

    message = struct(Message, message)
    GenServer.cast(handler_pid, {:mqtt, :publish, message.topic, message.payload})

    :ok
  rescue
    e ->
      Logger.error("Got error in handle_publish: #{inspect(e)}")
      :ok
  end

  # ----------------------------------------------------------------------------
  # Helpers
  # ----------------------------------------------------------------------------

  @spec log_error(result :: any, message :: String.t()) :: any
  defp log_error({:error, reason} = result, message) do
    Logger.error("[MqttPotion] #{message}: #{reason}")
    result
  end

  defp log_error(result, _message) do
    result
  end

  @spec connect(state :: State.t()) :: {:ok, State.t()} | {:error, String.t()}
  defp connect(%State{} = state) do
    Logger.debug("[MqttPotion] Connecting to #{state.opts[:host]}:#{state.opts[:port]}")

    opts = map_opts(state.opts)

    with(
      {:ok, conn_pid} when is_pid(conn_pid) <- :emqtt.start_link(opts),
      {:ok, _props} <- :emqtt.connect(conn_pid)
    ) do
      Logger.info("[MqttPotion] Connected #{inspect(conn_pid)}")
      state = %State{state | conn_pid: conn_pid}

      state = process_outgoing_queue(state)

      Enum.each(state.subscriptions, fn subscription ->
        case sub(state, subscription) do
          :ok -> :ok
          {:error, _reason} = error -> log_error(error, "subscribe error")
        end
      end)

      GenServer.cast(state.handler_pid, {:mqtt, :connect})

      {:ok, state}
    else
      {:error, reason} ->
        {:error, "#{inspect(reason)}"}
    end
  end

  @spec pub(state :: State.t(), topic :: String.t(), message :: String.t(), opts :: pub_opts()) ::
          :ok | {:error, String.t()}
  defp pub(%State{} = state, topic, message, opts) do
    if Keyword.get(opts, :qos, 0) == 0 do
      :ok = :emqtt.publish(state.conn_pid, topic, message, opts)
    else
      {:ok, _packet_id} = :emqtt.publish(state.conn_pid, topic, message, opts)
      :ok
    end
  catch
    :exit, value -> {:error, "The emqtt #{inspect(state.conn_pid)} is dead: #{inspect(value)}"}
  end

  @spec pub_or_queue(
          state :: State.t(),
          topic :: String.t(),
          message :: String.t(),
          opts :: pub_opts()
        ) ::
          :ok | {:queued, State.t()} | {:error, String.t()}
  defp pub_or_queue(%State{} = state, topic, message, opts) do
    if state.conn_pid == nil do
      Logger.info("[MqttPotion] Queuing outgoing message #{topic}")
      queue = state.outgoing
      head = {topic, message, opts}
      state = %{state | outgoing: [head | queue]}
      {:queued, state}
    else
      pub(state, topic, message, opts)
    end
  end

  @spec process_outgoing_queue(state :: State.t()) :: State.t()
  defp process_outgoing_queue(state) do
    queue = state.outgoing |> Enum.reverse()

    Enum.each(queue, fn {topic, message, opts} ->
      Logger.info("[MqttPotion] Publishing queued message #{topic}")

      pub(state, topic, message, opts)
      |> log_error("publish queue error")
    end)

    %{state | outgoing: []}
  end

  @spec sub(state :: State.t(), subscription :: subscription()) :: :ok | {:error, String.t()}
  defp sub(%State{} = state, {topic, opts}) do
    case :emqtt.subscribe(state.conn_pid, topic, opts) do
      {:ok, _props, [reason_code]} when reason_code in [0x00, 0x01, 0x02] ->
        Logger.info("[MqttPotion] Subscribed to #{topic} @ opts #{inspect(opts)}")
        :ok

      {:ok, _props, reason_codes} ->
        msg = "Subscription to #{topic} @ opts #{inspect(opts)} failed: #{inspect(reason_codes)}"
        {:error, msg}
    end
  catch
    :exit, value -> {:error, "The emqtt #{inspect(state.conn_pid)} is dead: #{inspect(value)}"}
  end

  @spec sub_or_nop(state :: State.t(), subscription :: subscription()) ::
          :ok | {:error, String.t()}
  defp sub_or_nop(%State{} = state, {topic, opts}) do
    if state.conn_pid == nil do
      Logger.info("[MqttPotion] Skipping subscription to #{topic} @ opts #{inspect(opts)}")
      :ok
    else
      sub(state, {topic, opts})
    end
  end

  @spec unsub(state :: State.t(), topic :: String.t()) :: :ok | {:error, String.t()}
  defp unsub(%State{} = state, topic) do
    case :emqtt.unsubscribe(state.conn_pid, topic) do
      {:ok, _props, [0x00]} ->
        Logger.info("[MqttPotion] Unsubscribed from #{topic}")
        :ok

      {:ok, _props, reason_codes} ->
        msg = "[MqttPotion] Unsubscribe from #{topic} failed #{inspect(reason_codes)}"
        {:error, msg}
    end
  catch
    :exit, value -> {:error, "The emqtt #{inspect(state.conn_pid)} is dead: #{inspect(value)}"}
  end

  @spec unsub_or_nop(state :: State.t(), topic :: String.t()) :: :ok | {:error, String.t()}
  defp unsub_or_nop(%State{} = state, topic) do
    if state.conn_pid == nil do
      Logger.info("[MqttPotion] Skipping unsubscribe from #{topic}")
      :ok
    else
      unsub(state, topic)
    end
  end

  defp dc(%State{} = state) do
    :ok = :emqtt.disconnect(state.conn_pid)
  catch
    :exit, _ -> :ok
  end

  ## Utility

  defp take_opts(opts) do
    case Keyword.split(opts, @opt_keys) do
      {keep, []} -> keep
      {_keep, extra} -> raise ArgumentError, "Unrecognized options #{inspect(extra)}"
    end
  end

  # Backoff with full jitter (after 3 attempts).
  defp retry_delay(initial_delay, _max_delay, attempt) when attempt < 3 do
    initial_delay
  end

  defp retry_delay(initial_delay, max_delay, attempt) when attempt < 1000 do
    temp = min(max_delay, initial_delay * pow(2, attempt))
    trunc(temp / 2 + Enum.random([0, temp / 2]))
  end

  defp retry_delay(_initial_delay, max_delay, _attempt) do
    max_delay
  end

  # Integer powers:
  # https://stackoverflow.com/a/44065965/2066155
  defp pow(n, k), do: pow(n, k, 1)
  defp pow(_, 0, acc), do: acc
  defp pow(n, k, acc), do: pow(n, k - 1, n * acc)

  # emqtt has some odd opt names and some erlang types that we'll want to
  # redefine but then map to emqtt expected format.
  defp map_opts(opts) do
    Enum.reduce(opts, [], &map_opt/2)
  end

  defp map_opt({:clean_session, val}, opts) do
    [{:clean_start, val} | opts]
  end

  defp map_opt({:client_id, val}, opts) do
    [{:clientid, val} | opts]
  end

  defp map_opt({:handler_functions, val}, opts) do
    [{:msg_handler, val} | opts]
  end

  defp map_opt({:host, val}, opts) do
    [{:host, to_charlist(val)} | opts]
  end

  defp map_opt({:hosts, hosts}, opts) do
    hosts = Enum.map(hosts, fn {host, port} -> {to_charlist(host), port} end)
    [{:hosts, hosts} | opts]
  end

  defp map_opt({:protocol_version, val}, opts) do
    version =
      case val do
        3 -> :v3
        4 -> :v4
        5 -> :v5
      end

    [{:proto_ver, version} | opts]
  end

  defp map_opt({:ws_path, val}, opts) do
    [{:ws_path, to_charlist(val)} | opts]
  end

  defp map_opt(opt, opts) do
    [opt | opts]
  end
end
