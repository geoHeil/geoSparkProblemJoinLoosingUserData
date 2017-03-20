// Copyright (C) 2017 Georg Heiler
package problemGeospark.inputMapper

import com.vividsolutions.jts.geom._
import com.vividsolutions.jts.io.WKTReader
import org.apache.log4j.Logger
import org.apache.spark.sql.{ Dataset, SparkSession }
import org.json4s.ParserUtil.ParseException

import scala.collection.generic.Growable
import scala.collection.mutable

case class PolygonGeoObject(id1: String, idValue: Int, wktString: String)

object PolygonGeoObjectMapper {
  @transient lazy val logger = Logger.getLogger(this.getClass)

  def read(spark: SparkSession, inputPath: String): Dataset[PolygonGeoObject] = {
    logger.debug(s"reading proptima pegaplan for path $inputPath")
    import spark.implicits._
    spark.read.
      option("header", "false")
      .option("inferSchema", true)
      .option("charset", "UTF-8")
      .option("delimiter", ";")
      .csv(inputPath)
      .withColumnRenamed("_c0", "id1")
      .withColumnRenamed("_c1", "idValue")
      .withColumnRenamed("_c2", "wktString")
      .na.drop
      .as[PolygonGeoObject]
  }

  def map(iterator: Iterator[PolygonGeoObject]): Iterator[Polygon] = {
    @transient lazy val reader = new WKTReader()
    iterator.flatMap(p => {
      try {
        reader.read(p.wktString) match {
          case m: MultiPolygon => {
            logger.debug("using multipolygon")
            val result: collection.Seq[Polygon] with Growable[Polygon] = mutable.Buffer[Polygon]()
            var i = 0
            while (i < m.getNumGeometries) {
              val intermediateGeoObjectPolygon = m.getGeometryN(i).asInstanceOf[Polygon]
              intermediateGeoObjectPolygon.setUserData(p.id1 + ";" + p.idValue.toString)
              logger.debug(s"using polygon $intermediateGeoObjectPolygon with user data section of ${p.id1 + ";" + p.idValue.toString}")
              result += intermediateGeoObjectPolygon
              i += 1
            }
            result.iterator
          }
          case poly: Polygon => {
            logger.debug(s"using user data of ${p.id1 + ";" + p.idValue.toString}")
            poly.setUserData(p.id1 + ";" + p.idValue.toString)
            Seq(poly).iterator
          }
        }
      } catch {
        case e: ParseException => {
          logger.error("Could not parse")
          logger.error(e.getCause)
          logger.error(e.getMessage)
          Seq().iterator
        }
      }
    })
  }
}
