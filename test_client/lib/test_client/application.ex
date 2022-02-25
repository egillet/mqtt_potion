defmodule TestClient.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    mqtt_host = Application.get_env(:test_client, :mqtt_host)
    mqtt_port = Application.get_env(:test_client, :mqtt_port)
    mqtt_user_name = Application.get_env(:test_client, :mqtt_user_name)
    mqtt_password = Application.get_env(:test_client, :mqtt_password)
    mqtt_ca_cert_file = Application.get_env(:test_client, :mqtt_ca_cert_file)

    opts = [
      name: TestClient.Mqtt,
      host: mqtt_host,
      port: mqtt_port,
      ssl: true,
      protocol_version: 5,
      client_id: "test_client",
      username: mqtt_user_name,
      password: mqtt_password,
      tcp_opts: [
        :inet6
      ],
      ssl_opts: [
        verify: :verify_peer,
        cacertfile: mqtt_ca_cert_file
      ],
      handler_pid: self,
      subscriptions: [
        {"test/1", 0}
      ]
    ]

    children = [
      {MqttPotion.Connection, opts}
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: TestClient.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
