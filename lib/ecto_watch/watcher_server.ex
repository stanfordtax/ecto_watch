defmodule EctoWatch.WatcherServer do
  alias EctoWatch.Helpers
  alias EctoWatch.WatcherOptions
  alias EctoWatch.WatcherSQL

  use GenServer
  alias EctoWatch.WatcherServer, as: State

  defstruct [:repo_mod, :pub_sub_mod, :pub_sub_topic, :watcher_options]

  def pub_sub_subscription_details(schema_mod_or_label, update_type, id) do
    name = Helpers.unique_label(schema_mod_or_label, update_type)

    if Process.whereis(name) do
      {:ok,
       GenServer.call(name, {:pub_sub_subscription_details, schema_mod_or_label, update_type, id})}
    else
      {:error, "No watcher found for #{inspect(schema_mod_or_label)} / #{inspect(update_type)}"}
    end
  end

  def start_link({repo_mod, pub_sub_mod, watcher_options}) do
    GenServer.start_link(
      __MODULE__,
      %State{
        repo_mod: repo_mod,
        pub_sub_mod: pub_sub_mod,
        pub_sub_topic: Atom.to_string(Helpers.unique_label(watcher_options)),
        watcher_options: watcher_options
      },
      name: Helpers.unique_label(watcher_options)
    )
  end

  def init(%State{repo_mod: repo_mod, watcher_options: watcher_options} = server) do
    Ecto.Adapters.SQL.query!(
      repo_mod,
      WatcherSQL.create_or_replace_function(watcher_options),
      []
    )

    Ecto.Adapters.SQL.query!(
      repo_mod,
      WatcherSQL.create_or_replace_trigger(watcher_options),
      []
    )

    notifications_pid = Process.whereis(:ecto_watch_postgrex_notifications)

    {:ok, _notifications_ref} =
      Postgrex.Notifications.listen(notifications_pid, "#{Helpers.unique_label(watcher_options)}")

    {:ok, server}
    # {:ok,
    #  %{
    #    pub_sub_mod: pub_sub_mod,
    #    unique_label: unique_label,
    #    schema_mod: watcher_options.schema_mod,
    #    schema_mod_or_label: watcher_options.label || watcher_options.schema_mod
    #  }}
  end

  def handle_call(
        {:pub_sub_subscription_details, schema_mod_or_label, update_type, pk},
        _from,
        %State{watcher_options: watcher_options} = state
      ) do
    unique_label = Helpers.unique_label(schema_mod_or_label, update_type)

    channel_name =
      if pk do
        ordered_values =
          pk
          |> Enum.sort_by(fn {k, _v} -> k end)
          |> Enum.map(fn {_k, v} -> v end)

        Enum.join(["#{Helpers.unique_label(watcher_options)}" | ordered_values], ":")
      else
        "#{unique_label}"
      end

    {:reply, {state.pub_sub_mod, channel_name}, state}
  end

  def handle_info(
        {:notification, _pid, _ref, channel_name, payload},
        %State{watcher_options: watcher_options, pub_sub_topic: pub_sub_topic} = state
      ) do
    if channel_name != pub_sub_topic do
      raise "Expected to receive message from #{inspect(pub_sub_topic)}, but received from #{inspect(channel_name)}"
    end

    primary_key = watcher_options.schema_mod.__schema__(:primary_key)
    label = watcher_options.label

    %{"type" => type, "columns" => columns} = Jason.decode!(payload)

    columns = Map.new(columns, fn {k, v} -> {String.to_existing_atom(k), v} end)

    specific_topic =
      Enum.join([state.pub_sub_topic | Enum.sort(Enum.map(primary_key, &columns[&1]))], ":")

    case type do
      "inserted" ->
        Phoenix.PubSub.broadcast(
          state.pub_sub_mod,
          state.pub_sub_topic,
          {:inserted, label, columns}
        )

      "updated" ->
        Phoenix.PubSub.broadcast(
          state.pub_sub_mod,
          specific_topic,
          {:updated, label, columns}
        )

        Phoenix.PubSub.broadcast(
          state.pub_sub_mod,
          state.pub_sub_topic,
          {:updated, label, columns}
        )

      "deleted" ->
        Phoenix.PubSub.broadcast(
          state.pub_sub_mod,
          specific_topic,
          {:deleted, label, columns}
        )

        Phoenix.PubSub.broadcast(
          state.pub_sub_mod,
          state.pub_sub_topic,
          {:deleted, label, columns}
        )
    end

    {:noreply, state}
  end

  def name(%WatcherOptions{} = watcher_options) do
    Helpers.unique_label(watcher_options)
  end
end
