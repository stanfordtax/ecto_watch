defmodule EctoWatch.WatcherSQL do
  alias EctoWatch.Helpers
  alias EctoWatch.WatcherOptions

  def create_or_replace_function(
        %WatcherOptions{
          schema_mod: schema_mod,
          update_type: update_type,
          extra_columns: extra_columns
        } = watcher_options
      ) do
    schema_name =
      case schema_mod.__schema__(:prefix) do
        nil -> "public"
        prefix -> prefix
      end

    unique_label = "#{unique_label(watcher_options)}"
    extra_columns = extra_columns || []

    columns_sql =
      Enum.uniq(schema_mod.__schema__(:primary_key) ++ extra_columns)
      |> Enum.map_join(",", &"'#{&1}',row.#{&1}")

    """
    CREATE OR REPLACE FUNCTION \"#{schema_name}\".#{unique_label}_func()
      RETURNS trigger AS $trigger$
      DECLARE
        row record;
        payload TEXT;
      BEGIN
        row := COALESCE(NEW, OLD);
        payload := jsonb_build_object('type','#{update_type}','columns',json_build_object(#{columns_sql}));
        PERFORM pg_notify('#{unique_label}', payload);

        RETURN NEW;
      END;
      $trigger$ LANGUAGE plpgsql;
    """
  end

  # To make things simple: generate a single string which is unique for each watcher
  # that can be used as the watcher process name, trigger name, trigger function name,
  # and Phoenix.PubSub channel name.
  def unique_label(%WatcherOptions{} = watcher_options) do
    unique_label(
      watcher_options.label || watcher_options.schema_mod,
      watcher_options.update_type
    )
  end

  defp unique_label(schema_mod_or_label, update_type) do
    label = Helpers.label(schema_mod_or_label)

    :"ew_#{update_type}_for_#{label}"
  end
end
