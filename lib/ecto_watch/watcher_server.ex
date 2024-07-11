defmodule EctoWatch.WatcherServer do
  alias EctoWatch.Helpers
  alias EctoWatch.WatcherOptions
  alias EctoWatch.WatcherSQL

  use GenServer

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
    GenServer.start_link(__MODULE__, {repo_mod, pub_sub_mod, watcher_options},
      name: Helpers.unique_label(watcher_options)
    )
  end

  def init({repo_mod, pub_sub_mod, watcher_options}) do
    unique_label = "#{Helpers.unique_label(watcher_options)}"

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
    {:ok, _notifications_ref} = Postgrex.Notifications.listen(notifications_pid, unique_label)

    {:ok,
     %{
       pub_sub_mod: pub_sub_mod,
       unique_label: unique_label,
       schema_mod: watcher_options.schema_mod,
       schema_mod_or_label: watcher_options.label || watcher_options.schema_mod
     }}
  end

  def handle_call(
        {:pub_sub_subscription_details, schema_mod_or_label, update_type, pk},
        _from,
        state
      ) do
    unique_label = Helpers.unique_label(schema_mod_or_label, update_type)

    channel_name =
      if pk do
        ordered_values =
          pk
          |> Enum.sort_by(fn {k, _v} -> k end)
          |> Enum.map(fn {_k, v} -> v end)

        Enum.join([state.unique_label | ordered_values], ":")
      else
        "#{unique_label}"
      end

    {:reply, {state.pub_sub_mod, channel_name}, state}
  end

  def handle_info({:notification, _pid, _ref, channel_name, payload}, state) do
    if channel_name != state.unique_label do
      raise "Expected to receive message from #{state.unique_label}, but received from #{channel_name}"
    end

    primary_key = state.schema_mod.__schema__(:primary_key)

    %{"type" => type, "columns" => columns} = Jason.decode!(payload)

    columns = Map.new(columns, fn {k, v} -> {String.to_existing_atom(k), v} end)

    specific_topic =
      Enum.join([state.unique_label | Enum.sort(Enum.map(primary_key, &columns[&1]))], ":")

    case type do
      "inserted" ->
        Phoenix.PubSub.broadcast(
          state.pub_sub_mod,
          state.unique_label,
          {:inserted, state.schema_mod_or_label, columns}
        )

      "updated" ->
        Phoenix.PubSub.broadcast(
          state.pub_sub_mod,
          specific_topic,
          {:updated, state.schema_mod_or_label, columns}
        )

        Phoenix.PubSub.broadcast(
          state.pub_sub_mod,
          state.unique_label,
          {:updated, state.schema_mod_or_label, columns}
        )

      "deleted" ->
        Phoenix.PubSub.broadcast(
          state.pub_sub_mod,
          specific_topic,
          {:deleted, state.schema_mod_or_label, columns}
        )

        Phoenix.PubSub.broadcast(
          state.pub_sub_mod,
          state.unique_label,
          {:deleted, state.schema_mod_or_label, columns}
        )
    end

    {:noreply, state}
  end

  def name(%WatcherOptions{} = watcher_options) do
    Helpers.unique_label(watcher_options)
  end
end
