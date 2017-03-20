// Copyright (C) 2017 Georg Heiler
package problemGeospark.rddMapper

import com.vividsolutions.jts.geom.{ Point, Polygon }
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.{ collect_list, udf }

import scala.collection.JavaConverters._

case class JoinResultEmptyUserData(a: String, c: Int, pointIds: Array[String])

object JoinResultMapper {
  def mapToBasicTypes(iterator: Iterator[(Polygon, java.util.HashSet[Point])]): Iterator[JoinResultEmptyUserData] = {
    iterator.map(k => {
      // TODO check if object creation is efficient / if creation of objects could be avoided
      val Array(a, c) = k._1.getUserData.asInstanceOf[String].split(";")
      val joinedPoints = k._2.asScala.map(_.getUserData.asInstanceOf[String]).toArray
      JoinResultEmptyUserData(a, c.toInt, joinedPoints)
    })
  }

  def reduceToSingleMultiPolygon(df: Dataset[JoinResultEmptyUserData]): Dataset[JoinResultEmptyUserData] = {
    import df.sparkSession.implicits._
    val flatten = udf((xs: Seq[Seq[String]]) => xs.flatten.distinct)
    val jpoinedPoints = flatten(collect_list($"pointIds")).alias("pointIds")
    df.groupBy($"a", $"b", $"c").agg(jpoinedPoints).as[JoinResultEmptyUserData]
  }
}
