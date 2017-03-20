// Copyright (C) 2017 Georg Heiler

package problemGeospark.rddMapper

import java.util

import com.holdenkarau.spark.testing.{ DatasetSuiteBase, SharedSparkContext }
import com.vividsolutions.jts.geom.{ Coordinate, GeometryFactory, Point, Polygon }
import com.vividsolutions.jts.io.WKTReader
import org.apache.spark.storage.StorageLevel
import org.datasyslab.geospark.enums.{ GridType, IndexType }
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.spatialRDD.{ PointRDD, PolygonRDD }
import org.scalatest.{ FlatSpec, Matchers }
import problemGeospark.inputMapper.{ PointAddress, PointAddressMapper, PolygonGeoObject, PolygonGeoObjectMapper }

class JoinResultMapperTest extends FlatSpec with Matchers with SharedSparkContext with DatasetSuiteBase {
  "A JoinResultMapper" should "map an iterator of spatial join to plain type" in {
    val javaHashSet = new util.HashSet[Point]()
    val fact = new GeometryFactory()
    val reader = new WKTReader()

    val wktLineString = "Polygon((0 0, 0 20, 20 20, 20 0, 0 0),(5 5, 5 15, 15 15, 15 5, 5 5))"

    val p1 = fact.createPoint(new Coordinate(12, 40))
    val p2 = fact.createPoint(new Coordinate(23, 56))
    p1.setUserData("fooPoint1")
    p2.setUserData("fooTheBar")

    val geometry = reader.read(wktLineString) match {
      case poly: Polygon => {
        poly.setUserData("first;second;123")
        poly
      }
    }

    javaHashSet.add(p1)
    javaHashSet.add(p2)

    val result = JoinResultMapper.mapToBasicTypes(Seq((geometry, javaHashSet)).iterator).toSeq
    result(0).a should equal("first")
    result(0).b should equal("second")
    result(0).c should equal(123)
    result(0).pointIds should contain allOf ("fooPoint1", "fooTheBar")
  }

  it should "reduce polygons with same key to single multipolygon" in {
    import spark.implicits._
    val df = Seq(
      JoinResultEmptyUserData("A", "A", 123, Array("A")),
      JoinResultEmptyUserData("A", "A", 123, Array("B")),
      JoinResultEmptyUserData("B", "B", 123, Array("C", "A")),
      JoinResultEmptyUserData("B", "B", 123, Array("C", "E", "A")),
      JoinResultEmptyUserData("B", "B", 123, Array("D"))
    ).toDS

    val res = JoinResultMapper.reduceToSingleMultiPolygon(df).collect.toSeq

    res.head.a should equal("A")
    res.head.b should equal("A")
    res.head.c should equal(123)

    res.last.a should equal("B")
    res.last.b should equal("B")
    res.last.c should equal(123)

    res.head.pointIds should contain allOf ("A", "B")
    res.last.pointIds should contain allOf ("A", "C", "D", "E")
  }

  it should "have a join result for joined geometries in the user data section" in {
    import spark.implicits._

    val dfPoints = Seq(PointAddress("id1", 16.3262936894114, 47.9811515208272), PointAddress("id2", 16.330895, 47.975476)).toDS
    val dfPolygons = Seq(PolygonGeoObject("firstID", 123, "POLYGON ((16.326079996672 47.9812444466704, 16.326079996672 47.9801333355592, 16.3277466633388 47.9801333355592, 16.3277466633388 47.9812444466704, 16.326079996672 47.9812444466704))")).toDS

    dfPoints.show
    dfPolygons.show

    val pointObjects = new PointRDD(dfPoints.rdd.mapPartitions(PointAddressMapper.map), StorageLevel.MEMORY_ONLY)
    val polygonObjects = new PolygonRDD(dfPolygons.rdd.mapPartitions(PolygonGeoObjectMapper.map), StorageLevel.MEMORY_ONLY)

    pointObjects.spatialPartitioning(GridType.RTREE)
    pointObjects.buildIndex(IndexType.RTREE, true)
    polygonObjects.spatialPartitioning(pointObjects.grids)
    val joinResult = JoinQuery.SpatialJoinQuery(pointObjects, polygonObjects, true)
    joinResult.cache
    import scala.collection.JavaConverters._

    val local = joinResult.collect.asScala.toArray
    print(s"number of matched joined geo objects ${local.length}")
    val joinedPoints = local.map(k => {
      k._2.asScala.map(_.getUserData.asInstanceOf[String]).toArray
    })
    joinedPoints.length should not equal (0)
  }
}
