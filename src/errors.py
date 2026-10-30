"""Project-specific exceptions."""


class MetadataDriveError(Exception):
    """Base exception for the project."""


class ConfigurationError(MetadataDriveError):
    """Configuration is missing or invalid."""


class AuthenticationError(MetadataDriveError):
    """Google credentials could not be loaded or used."""


class DriveAPIError(MetadataDriveError):
    """A Google Drive API operation failed."""


class MetadataCapacityError(MetadataDriveError):
    """A reliable metadata payload capacity could not be established."""


class ChunkMissingError(MetadataDriveError):
    """A required dataset chunk is missing."""


class ChunkCorruptedError(MetadataDriveError):
    """A dataset chunk failed integrity validation."""


class IntegrityError(MetadataDriveError):
    """The restored dataset does not match its manifest."""


class DatasetNotFoundError(MetadataDriveError):
    """The requested dataset does not exist in the local index."""

