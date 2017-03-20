// Copyright (C) 2017 Georg Heiler

package problemGeospark.inputMapper

import com.holdenkarau.spark.testing.{ DatasetSuiteBase, SharedSparkContext }
import org.scalatest.FunSuite
import problemGeospark.inputMapper.PointAddressMapper

class PointAddressMapperTest extends FunSuite with SharedSparkContext with DatasetSuiteBase {
  test("test parsing of objects") {
    import spark.implicits._
    val regularData = Seq(PointAddress("A", 1.1, 2.2), PointAddress("B", 2.1, 4.5))
    val df = regularData.toDS
    val geoObjects = df.rdd.mapPartitions(PointAddressMapper.map).collect

    assert(geoObjects.length === regularData.length)
    assert(geoObjects.head.getUserData.asInstanceOf[String] === "A")
  }
}