import logging
from pathlib import Path, PurePosixPath

from azure.core.exceptions import AzureError, ResourceExistsError
from azure.storage.filedatalake import DataLakeServiceClient, FileSystemClient

from src.config.settings import settings

logger = logging.getLogger(__name__)

CSV_PATTERN = "*.csv"


def validate_landing_settings() -> None:
    """Ensure required Azure settings are present before ingestion starts."""

    settings.validate_required("endpoint", "access_key", "landing_container")


def get_landing_client() -> FileSystemClient:
    """Create a client for the configured landing filesystem."""

    logger.info("Creating Data Lake client")
    validate_landing_settings()

    endpoint = settings.get_required("endpoint")
    access_key = settings.get_required("access_key")
    landing_container = settings.get_required("landing_container")

    try:
        service_client = DataLakeServiceClient(
            account_url=endpoint,
            credential=access_key
        )

        client = service_client.get_file_system_client(
            file_system=landing_container
        )

        logger.info("Data Lake client created successfully")
        return client

    except AzureError as e:
        logger.exception("Failed to create Data Lake client: %s", e)
        raise


def resolve_local_path() -> Path:
    """Resolve and validate the local directory configured for landing ingestion."""

    local_path = Path(settings.get_required("local_path"))

    if not local_path.exists():
        raise FileNotFoundError(f"Local path not found: {local_path}")

    if not local_path.is_dir():
        raise NotADirectoryError(f"Local path is not a directory: {local_path}")

    return local_path


def ensure_remote_directory(
    file_system_client: FileSystemClient,
    folder_name: str,
) -> None:
    """Create the destination directory when it does not exist yet."""

    directory_client = file_system_client.get_directory_client(folder_name)

    try:
        directory_client.create_directory()
        logger.info("Directory created: %s", folder_name)
    except ResourceExistsError:
        logger.info("Directory already exists: %s", folder_name)


def list_csv_files(local_path: Path) -> list[Path]:
    """Return CSV files from the local landing directory in deterministic order."""

    return sorted(path for path in local_path.glob(CSV_PATTERN) if path.is_file())


def upload_file(
    file_system_client: FileSystemClient,
    folder_name: str,
    file_path: Path,
) -> None:
    """Upload a single local CSV file into the landing filesystem."""

    file_name = file_path.name
    remote_path = str(PurePosixPath(folder_name) / file_name)

    logger.info("Uploading file: %s", file_name)

    try:
        file_client = file_system_client.get_file_client(remote_path)

        with file_path.open("rb") as data:
            file_client.upload_data(data, overwrite=True)

        logger.info("Upload successful: %s", remote_path)

    except (AzureError, OSError):
        logger.exception("Failed to upload file: %s", remote_path)
        raise


def ingest_landing() -> None:
    """Upload local CSV files into the landing layer."""

    logger.info("Starting landing ingestion")

    file_system_client = get_landing_client()
    local_path = resolve_local_path()
    folder_name = local_path.name

    logger.info("Reading files from: %s", local_path)

    ensure_remote_directory(file_system_client, folder_name)

    files = list_csv_files(local_path)

    if not files:
        logger.warning("No CSV files found for ingestion")
        return

    logger.info("%s files found for upload", len(files))

    success = 0
    failed = 0

    for file_path in files:
        try:
            upload_file(file_system_client, folder_name, file_path)
            success += 1
        except (AzureError, OSError):
            failed += 1
            logger.warning("File upload failed and will be counted as error: %s", file_path.name)

    logger.info(
        "Ingestion finished | success=%s | failed=%s | total=%s",
        success,
        failed,
        len(files),
    )

    if failed > 0:
        raise RuntimeError(f"{failed} files failed during ingestion")
