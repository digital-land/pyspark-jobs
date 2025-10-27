from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from shapely import wkt


def get_centroid_wkt(geom_wkt):
    try:
        geom = wkt.loads(geom_wkt)
        centroid = geom.centroid
        return centroid.wkt
    except Exception:
        return None

centroid_udf = udf(get_centroid_wkt, StringType())