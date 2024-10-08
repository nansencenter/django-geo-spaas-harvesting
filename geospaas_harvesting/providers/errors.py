class MetadataNormalizationError(Exception):
    """Exception raised by normalizers when they cannot normalize
    some metadata
    """


class NoNormalizerFound(Exception):
    """Exception raised by a handler when it was not able to find a
    normalizer suited to normalize some metadata
    """
