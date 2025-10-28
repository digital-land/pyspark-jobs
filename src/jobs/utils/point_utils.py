"""Utility helpers for point geometry operations.

This module avoids importing heavy, platform-specific binary
extensions (Shapely, PySpark) at import time. Both Shapely and
PySpark are imported lazily so that simple imports of this module
in environments without those packages won't raise ModuleNotFoundError.
"""


def get_centroid_wkt(geom_wkt):
    """Return WKT of centroid for a WKT geometry string.

    Shapely is imported lazily so that importing this module doesn't fail
    in environments where Shapely is not installed (which caused
    ModuleNotFoundError: No module named 'shapely.lib'). If Shapely is not
    available at call time the function returns None.
    """
    try:
        # Import lazily to avoid import-time errors when Shapely C extensions
        # are not available in the runtime environment.
        from shapely import wkt  # type: ignore
    except Exception:
        # Shapely not available or failed to load compiled extensions
        return None

    try:
        geom = wkt.loads(geom_wkt)
        centroid = geom.centroid
        return centroid.wkt
    except Exception:
        return None


try:
    # Create a PySpark UDF if PySpark is available. If PySpark isn't
    # installed in the environment (common in lightweight test runners)
    # just expose None so importing this module doesn't fail.
    from pyspark.sql.functions import udf
    from pyspark.sql.types import StringType

    centroid_udf = udf(get_centroid_wkt, StringType())
except Exception:
    centroid_udf = None
