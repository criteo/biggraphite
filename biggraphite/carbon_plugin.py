from carbon import database
from carbon import exceptions
from biggraphite import accessor


class BigGraphiteDatabase(database.TimeSeriesDatabase):
    plugin_name = "biggraphite"

    def __init__(self, settings):
        keyspace = settings.get("keyspace")
        contact_points_str = settings.get("contact_points")

        if not keyspace:
            raise exceptions.CarbonConfigException("keyspace is mandatory")

        if not contact_points_str:
            raise exceptions.CarbonConfigException(
                "contact_points are mandatory")

        contact_points = [s.strip() for s in contact_points_str.split(",")]
        port = settings.get("port", 9042)

        self._accessor = accessor.Accessor(keyspace, contact_points, port)
        self._accessor.connect()

        # TODO: we may want to use/implement these
        # settings.WHISPER_AUTOFLUSH:
        # settings.WHISPER_SPARSE
        # settings.WHISPER_FALLOCATE_CREATE:
        # settings.WHISPER_LOCK_WRITES:

    def write(self, metric, datapoints):
        self._accessor.insert_points(
            metric_name=metric, timestamps_and_values=datapoints)

    def exists(self, metric):
        # If exists returns "False" then "create" will be called.
        # New metrics are also throttled by some settings.
        return True

    def create(self, metric, retentions, xfilesfactor, aggregation_method):
        pass

    def getMetadata(self, metric, key):
        if key != "aggregationMethod":
            raise ValueError("Unsupported metadata key \"%s\"" % key)

        return "average"

    def setMetadata(self, metric, key, value):
        return "average"

    def getFilesystemPath(self, metric):
        # Only used for logging.
        return metric
