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

    unique_label = "#{Helpers.unique_label(watcher_options)}"
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

  def create_or_replace_trigger(%WatcherOptions{schema_mod: schema_mod} = watcher_options) do
    unique_label = "#{Helpers.unique_label(watcher_options)}"

    schema_name =
      case schema_mod.__schema__(:prefix) do
        nil -> "public"
        prefix -> prefix
      end

    table_name = "#{schema_mod.__schema__(:source)}"

    update_keyword =
      case watcher_options.update_type do
        :inserted ->
          "INSERT"

        :updated ->
          trigger_columns = watcher_options.trigger_columns

          if trigger_columns do
            "UPDATE OF #{Enum.join(trigger_columns, ", ")}"
          else
            "UPDATE"
          end

        :deleted ->
          "DELETE"
      end

    """
    CREATE OR REPLACE TRIGGER #{unique_label}_trigger
      AFTER #{update_keyword} ON \"#{schema_name}\".\"#{table_name}\" FOR EACH ROW
      EXECUTE PROCEDURE \"#{schema_name}\".#{unique_label}_func();
    """
  end
end
