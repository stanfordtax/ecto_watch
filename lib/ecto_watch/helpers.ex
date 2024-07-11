defmodule EctoWatch.Helpers do
  alias EctoWatch.WatcherOptions

  # To make things simple: generate a single string which is unique for each watcher
  # that can be used as the watcher process name, trigger name, trigger function name,
  # and Phoenix.PubSub channel name.
  def unique_label(%WatcherOptions{} = watcher_options) do
    unique_label(
      watcher_options.label || watcher_options.schema_mod,
      watcher_options.update_type
    )
  end

  def unique_label(schema_mod_or_label, update_type) do
    label = label(schema_mod_or_label)

    :"ew_#{update_type}_for_#{label}"
  end

  def label(schema_mod_or_label) do
    if is_ecto_schema_mod?(schema_mod_or_label) do
      module_to_label(schema_mod_or_label)
    else
      schema_mod_or_label
    end
  end

  def module_to_label(module) do
    module
    |> Module.split()
    |> Enum.join("_")
    |> String.downcase()
  end

  def is_ecto_schema_mod?(schema_mod) do
    schema_mod.__schema__(:fields)

    true
  rescue
    UndefinedFunctionError -> false
  end
end
