namespace StreamApi.PostgreSQL;

public static class PostgreSqlTriggerDefinition
{
    public const string ChannelName =
        "streamapi_log_dt3";

    public const string FunctionName =
        "notify_streamapi_log_dt3_proses";

    public const string TriggerName =
        "trg_streamapi_log_dt3_proses";

    public const string InstallSql = """
        CREATE OR REPLACE FUNCTION public.notify_streamapi_log_dt3_proses()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            IF TG_OP = 'INSERT' THEN

                PERFORM pg_notify(
                    'streamapi_log_dt3',
                    json_build_object(
                        'kode_toko', NEW.kode_toko,
                        'jenis_file', NEW.jenis_file,
                        'tgl_dt3', NEW.tgl_dt3,
                        'jumlah_record', NEW.jumlah_record,
                        'tgl_proses', NEW.tgl_proses
                    )::text
                );

                RETURN NEW;
            END IF;

            IF TG_OP = 'UPDATE' THEN

                IF NEW.tgl_proses IS DISTINCT FROM OLD.tgl_proses THEN

                    PERFORM pg_notify(
                        'streamapi_log_dt3',
                        json_build_object(
                            'kode_toko', NEW.kode_toko,
                            'jenis_file', NEW.jenis_file,
                            'tgl_dt3', NEW.tgl_dt3,
                            'jumlah_record', NEW.jumlah_record,
                            'tgl_proses', NEW.tgl_proses
                        )::text
                    );

                END IF;

                RETURN NEW;
            END IF;

            RETURN NEW;
        END;
        $$;

        DROP TRIGGER IF EXISTS trg_streamapi_log_dt3_proses
        ON public.log_dt3_proses;

        CREATE TRIGGER trg_streamapi_log_dt3_proses
        AFTER INSERT OR UPDATE OF tgl_proses
        ON public.log_dt3_proses
        FOR EACH ROW
        EXECUTE FUNCTION public.notify_streamapi_log_dt3_proses();
        """;
}