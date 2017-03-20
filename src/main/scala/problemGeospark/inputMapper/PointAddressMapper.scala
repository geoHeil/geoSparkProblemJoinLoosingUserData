// Copyright (C) 2017 Georg Heiler
package problemGeospark.inputMapper

import com.vividsolutions.jts.geom.{ Coordinate, GeometryFactory, Point }
import org.apache.log4j.Logger
import org.apache.spark.sql.{ Dataset, SparkSession }

case class PointAddress(id: String, latitude: Double, longitude: Double)

object PointAddressMapper {

  @transient lazy val logger = Logger.getLogger(this.getClass)

  def read(spark: SparkSession, inputPath: String): Dataset[PointAddress] = {
    import spark.implicits._
    spark.read.
      option("header", "false")
      .option("inferSchema", true)
      .option("charset", "UTF-8")
      .option("delimiter", ",")
      .csv(inputPath)
      .withColumnRenamed("_c0", "id")
      .withColumnRenamed("_c1", "latitude")
      .withColumnRenamed("_c2", "longitude")
      .withColumn("latitude", $"latitude".cast("Double"))
      .withColumn("longitude", $"longitude".cast("Double"))
      .na.drop
      .as[PointAddress]
  }

  def map(iterator: Iterator[PointAddress]): Iterator[Point] = {
    @transient lazy val fact = new GeometryFactory()

    iterator.flatMap(p => {
      val geometry = fact.createPoint(new Coordinate(p.longitude, p.latitude))
      geometry.setUserData(p.id)
      List(geometry).iterator
    })
  }
}
