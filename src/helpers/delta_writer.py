from deltalake.writer import write_deltalake
import logging

logger = logging.getLogger(__name__)

def write_delta(
    path,
    table,
    mode="append",
    partition_by=None,
    target_file_size=None,
    settings=None
):
    try:
        storage_options = None

        if settings:
            storage_options = {
                "account_name": settings.account_name,
                "access_key": settings.access_key
            }

        kwargs = {
            "mode": mode,
        }

        if partition_by is not None:
            kwargs["partition_by"] = partition_by

        if target_file_size is not None:
            kwargs["target_file_size"] = target_file_size

        if storage_options is not None:
            kwargs["storage_options"] = storage_options

        result = write_deltalake(path, table, **kwargs)

        logger.info(f"Delta write successful: path={path}, mode={mode}")
        return result

    except Exception as e:
        logger.exception(
            f"Error writing Delta table at path={path} | mode={mode} | error={str(e)}"
        )
        raise