// Copyright (C) 2017 Georg Heiler

package problemGeospark

import java.io.File

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.magellan.dsl.expressions._
import org.apache.spark.storage.StorageLevel
import org.datasyslab.geospark.spatialRDD.PolygonRDD
import problemGeospark.inputMapper.{ PointAddressMapper, PolygonGeoObjectMapper }
import problemGeospark.rddMapper.LinestringMapper

object Magellan extends App {

  @transient lazy val logger: Logger = Logger.getLogger(this.getClass)

  val conf: SparkConf = new SparkConf()
    .setAppName("geomesaSparkInMemory")
    .setMaster("local[*]")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

  val spark: SparkSession = SparkSession
    .builder()
    .config(conf)
    //      .enableHiveSupport()
    .getOrCreate()

  import spark.implicits._

  val path = "src" + File.separator + "main" + File.separator + "resources" + File.separator
  val pointsPath = path + "points.csv"
  val polygonPath = path + "polygon_multipolygon.csv"

  val addressesCsv = PointAddressMapper.read(spark, pointsPath)
  val geometriesCsv = PolygonGeoObjectMapper.read(spark, polygonPath)
  //  addressesCsv.show
  //  geometriesCsv.show

  // TODO find a better way to read MULTIPOLYGONS to list of polygons
  val geometriesObjects = new PolygonRDD(geometriesCsv.rdd.mapPartitions(PolygonGeoObjectMapper.map), StorageLevel.MEMORY_ONLY)
  val geometriesCsvOnlyPolygonStrings = geometriesObjects.getRawSpatialRDD.rdd.mapPartitions(LinestringMapper.mapPolygonToLinestring).toDS
  geometriesCsvOnlyPolygonStrings.show

  // todo check that x,y lat long are not mixed up
  val addressesAsGeom = addressesCsv.withColumn("point", point('latitude, 'longitude))
  addressesAsGeom.show

  val geometriesCsvAsGeom = geometriesCsvOnlyPolygonStrings.withColumn("polygon", wkt('wktString))
  geometriesCsvAsGeom.show

  addressesAsGeom.join(geometriesCsvAsGeom).where($"point" within $"polygon").show

  addressesAsGeom.join(geometriesCsvAsGeom).where($"point" intersects $"polygon").show

}
