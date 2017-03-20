// Copyright (C) 2017 Georg Heiler

package problemGeospark.rddMapper

import com.vividsolutions.jts.geom.Polygon
import com.vividsolutions.jts.io.WKTWriter

case class PolygonLinestring(id: String, value: Int, wktString: String)

object LinestringMapper {
  def mapPolygonToLinestring(iterator: Iterator[AnyRef]): Iterator[PolygonLinestring] = {
    lazy val writer = new WKTWriter()
    iterator.flatMap(cur => {

      cur match {
        case p: Polygon => {
          val Array(siteName, value) = p.getUserData.asInstanceOf[String].split(";")
          Seq(PolygonLinestring(siteName, value.toInt, writer.write(p)))
        }
      }
    })
  }
}
