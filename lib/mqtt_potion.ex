defmodule MqttPotion do
  @moduledoc """
  Public Interface for MQTT client.
  """

  require Logger

  @type opts :: [
          {:name, atom}
          | {:owner, pid}
          | {:handler, module}
          | {:host, binary}
          | {:hosts, [{binary, :inet.port_number()}]}
          | {:port, :inet.port_number()}
          | {:tcp_opts, [:gen_tcp.option()]}
          | {:ssl, boolean}
          | {:ssl_opts, [keyword()]}
          | {:ws_path, binary}
          | {:connect_timeout, pos_integer}
          | {:bridge_mode, boolean}
          | {:client_id, iodata}
          | {:clean_start, boolean}
          | {:username, iodata}
          | {:password, iodata}
          | {:protocol_version, 3 | 4 | 5}
          | {:keepalive, non_neg_integer}
          | {:max_inflight, pos_integer}
          | {:retry_interval, timeout}
          | {:will_topic, iodata}
          | {:will_payload, iodata}
          | {:will_retain, boolean}
          | {:will_qos, pos_integer}
          | {:will_props, %{atom => term}}
          | {:auto_ack, boolean}
          | {:ack_timeout, pos_integer}
          | {:force_ping, boolean}
          | {:properties, %{atom => term}}
          | {:reconnect, {delay :: non_neg_integer, max_delay :: non_neg_integer}}
          | {:subscriptions, [subscription()]}
          | {:start_when, {mfa, retry_in :: non_neg_integer}}
        ]

  @type pub_opts :: list(:emqtt.pubopt())
  @type sub_opts :: list(:emqtt.subopt())
  @type subscription ::
          {topic :: binary, qos :: 0 | 1 | 2} | {topic :: binary, opts :: sub_opts()}

  defmodule Message do
    @moduledoc """
    Defines an incoming message
    """

    @type t :: %__MODULE__{
            topic: String.t(),
            payload: String.t(),
            retain: boolean()
          }
    @enforce_keys [:topic, :payload, :retain]
    defstruct [:topic, :payload, :retain]
  end

  ## Async

  @spec publish(
          server :: GenServer.server(),
          topic :: String.t(),
          message :: String.t(),
          opts :: pub_opts()
        ) :: :ok
  def publish(server, topic, message, opts \\ []) do
    GenServer.cast(server, {:publish, topic, message, opts})
  end

  @spec subscribe(server :: GenServer.server(), subscription :: subscription()) :: :ok
  def subscribe(server, subscription) do
    GenServer.cast(server, {:subscribe, subscription})
  end

  @spec unsubscribe(server :: GenServer.server(), topic :: String.t()) :: :ok
  def unsubscribe(server, topic) do
    GenServer.cast(server, {:unsubscribe, topic})
  end

  @spec disconnect(server :: GenServer.server()) :: :ok
  def disconnect(server) do
    GenServer.cast(server, :disconnect)
  end

  ## Sync

  @spec publish_sync(
          server :: GenServer.server(),
          topic :: String.t(),
          message :: String.t(),
          opts :: pub_opts()
        ) :: :ok | {:error, String.t()}
  def publish_sync(server, topic, message, opts \\ []) do
    GenServer.call(server, {:publish, topic, message, opts})
  end

  @spec subscribe_sync(server :: GenServer.server(), subscription :: subscription()) ::
          :ok | {:error, String.t()}
  def subscribe_sync(server, subscription) do
    GenServer.call(server, {:subscribe, subscription})
  end

  @spec unsubscribe_sync(server :: GenServer.server(), topic :: String.t()) ::
          :ok | {:error, String.t()}
  def unsubscribe_sync(server, topic) do
    GenServer.call(server, {:unsubscribe, topic})
  end

  @spec disconnect_sync(server :: GenServer.server()) :: :ok | {:error, String.t()}
  def disconnect_sync(server) do
    GenServer.call(server, :disconnect)
  end
end
