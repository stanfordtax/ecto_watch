defmodule EctoWatch.WatcherOptions do
  @moduledoc """
  The options for subscribing to a particular table.

  ## Options
    * `:schema_mod`: The schema module, for example `MyApp.User` to listen to changes for
    * `:update_type`: The type of change to listen for
    * `:label`: An optional label that defaults to the scheam module
    * `:trigger_columns`: These columns, when changed, will trigger an event. Defaults to all of the columns.
    * `:return_columns`: These columns will be returned as part of the payload. Defaults to the primary key.
    * `:subscribe_columns`: If subscribing to a specific topic, these are the filter criteria that will be used. Defaults to the primary key.
  """
  defstruct [
    :schema_mod,
    :update_type,
    :label,
    :trigger_columns,
    :return_columns,
    :subscribe_columns
  ]

  def new({schema_mod, update_type}) do
    new({schema_mod, update_type, []})
  end

  def new({schema_mod, update_type, opts}) do
    opts =
      [schema_mod: schema_mod, update_type: update_type]
      |> Keyword.merge(opts)
      |> Keyword.put_new(:label, schema_mod)
      |> Keyword.put_new(:subscribe_columns, schema_mod.__schema__(:primary_key))

    struct!(__MODULE__, opts)
  end

  def validate_list([]) do
    {:error, "requires at least one watcher"}
  end

  def validate_list(list) when is_list(list) do
    result =
      list
      |> Enum.map(&validate/1)
      |> Enum.find(&match?({:error, _}, &1))

    result || {:ok, list}
  end

  def validate_list(_) do
    {:error, "should be a list"}
  end

  defp validate({schema_mod, update_type}) do
    validate({schema_mod, update_type, []})
  end

  defp validate({schema_mod, update_type, opts}) do
    opts =
      opts
      |> Keyword.put(:schema_mod, schema_mod)
      |> Keyword.put(:update_type, update_type)

    schema = [
      schema_mod: [
        type: {:custom, __MODULE__, :validate_schema_mod, []},
        required: true
      ],
      update_type: [
        type: {:in, ~w[inserted updated deleted]a},
        required: true
      ],
      label: [
        type: :atom,
        required: false
      ],
      trigger_columns: [
        type:
          {:custom, __MODULE__, :validate_trigger_columns,
           [opts[:label], schema_mod, update_type]},
        required: false
      ],
      return_columns: [
        type: {:custom, __MODULE__, :validate_columns, [schema_mod]},
        required: false
      ],
      subscribe_columns: [
        type: {:custom, __MODULE__, :validate_columns, [schema_mod]},
        required: false
      ]
    ]

    with {:error, error} <- NimbleOptions.validate(opts, schema) do
      {:error, Exception.message(error)}
    end
  end

  defp validate(other) do
    {:error,
     "should be either `{schema_mod, update_type}` or `{schema_mod, update_type, opts}`.  Got: #{inspect(other)}"}
  end

  def validate_schema_mod(schema_mod) when is_atom(schema_mod) do
    if EctoWatch.Helpers.is_ecto_schema_mod?(schema_mod) do
      {:ok, schema_mod}
    else
      {:error, "Expected schema_mod to be an Ecto schema module. Got: #{inspect(schema_mod)}"}
    end
  end

  def validate_schema_mod(_), do: {:error, "should be an atom"}

  def validate_trigger_columns(columns, label, schema_mod, update_type) do
    cond do
      update_type != :updated ->
        {:error, "Cannot listen to trigger_columns for `#{update_type}` events."}

      label == nil ->
        {:error, "Label must be used when trigger_columns are specified."}

      true ->
        validate_columns(columns, schema_mod)
    end
  end

  def validate_columns([], _schema_mod),
    do: {:error, "List must not be empty"}

  def validate_columns(columns, schema_mod) do
    schema_fields = schema_mod.__schema__(:fields)

    Enum.reject(columns, &(&1 in schema_fields))
    |> case do
      [] ->
        {:ok, columns}

      extra_fields ->
        {:error, "Invalid columns for #{inspect(schema_mod)}: #{inspect(extra_fields)}"}
    end
  end
end
