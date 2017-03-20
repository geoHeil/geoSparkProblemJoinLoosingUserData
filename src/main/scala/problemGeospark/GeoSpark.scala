// Copyright (C) 2017 Georg Heiler

package problemGeospark

import java.io.File

import com.vividsolutions.jts.geom.{ Geometry, Point, Polygon }
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.datasyslab.geospark.enums.{ GridType, IndexType }
import org.datasyslab.geospark.joinJudgement.GeometryByPolygonJudgementUsingIndex
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.spatialPartitioning.DuplicatesHandler
import org.datasyslab.geospark.spatialRDD.{ PointRDD, PolygonRDD }
import problemGeospark.inputMapper.{ PointAddressMapper, PolygonGeoObjectMapper }
import problemGeospark.rddMapper.JoinResultMapper

object GeoSpark extends App {
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
  addressesCsv.show
  geometriesCsv.show

  val pointObjects = new PointRDD(addressesCsv.rdd.mapPartitions(PointAddressMapper.map), StorageLevel.MEMORY_ONLY)
  val polygonObjects = new PolygonRDD(geometriesCsv.rdd.mapPartitions(PolygonGeoObjectMapper.map), StorageLevel.MEMORY_ONLY)

  // both RDDs still contain valid user data
  //  println("both rdd still contain valid user data")
  //  println(pointObjects.getRawSpatialRDD.first.asInstanceOf[Point].getUserData)
  //  println(polygonObjects.getRawSpatialRDD.first.asInstanceOf[Polygon].getUserData)

  pointObjects.spatialPartitioning(GridType.RTREE)
  pointObjects.buildIndex(IndexType.RTREE, true)
  polygonObjects.spatialPartitioning(pointObjects.grids)
  val joinResult = JoinQuery.SpatialJoinQuery(pointObjects, polygonObjects, true)
  joinResult.cache
  println(s"join count ${joinResult.count}")
  println(s"join RDD count ${joinResult.rdd.count}")

  // lets dive in deeper into the geoSpark code
  import scala.collection.JavaConverters._

  // in fact all user data fields are empty
  val local = joinResult.collect.asScala.map(k => {
    k._2.asScala.map(_.getUserData.asInstanceOf[String]).toArray
  })
  joinResult.first._2.asScala.map(_.getUserData.asInstanceOf[String]).toArray.foreach(println)
  println(s"join size ${local.size}")

  // trying to reproduce the problem
  //  val cogroupResult = pointObjects.indexedRDD.cogroup(polygonObjects.spatialPartitionedRDD)
  //  val joinResultWithDuplicates = cogroupResult.flatMapToPair(new GeometryByPolygonJudgementUsingIndex())
  //  val joinListResultAfterAggregation = DuplicatesHandler.removeDuplicatesGeometryByPolygon(joinResultWithDuplicates)

  // TODO convert to scala
  //  val castedResult = joinListResultAfterAggregation.mapValues(new Function[java.util.HashSet[Geometry], java.util.HashSet[Point]]() {
  //    def call(spatialObjects: java.util.HashSet[Geometry]): java.util.HashSet[Point] = {
  //      val castedSpatialObjects = new java.util.HashSet[Point]
  //      val spatialObjectIterator = spatialObjects.iterator
  //      while (spatialObjectIterator.hasNext) castedSpatialObjects.add(spatialObjectIterator.next.asInstanceOf[Point])
  //      castedSpatialObjects
  //    }
  //  })

  //  joinListResultAfterAggregation.rdd.first._2.asScala.map(k => {
  //    println("gfmdkflkfdkjfzlkjfk")
  //    println(k.asInstanceOf[java.util.HashSet[Point]].asScala.map(_.getUserData)) //.asInstanceOf[String]).toArray
  //  })

  // this will show a data frame, but the last field is empty.
  val basicTypes = joinResult.rdd.mapPartitions(JoinResultMapper.mapToBasicTypes).toDS
  basicTypes.show

  spark.stop
}
