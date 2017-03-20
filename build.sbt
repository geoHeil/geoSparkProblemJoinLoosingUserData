name := "geosparkProblems"
organization := "problemGeospark"

scalaVersion := "2.11.8"

scalacOptions ++= Seq(
  "-target:jvm-1.8",
  "-encoding", "UTF-8",
  "-unchecked",
  "-deprecation",
  "-feature",
  "-Xfuture",
  "-Xlint:missing-interpolator",
  "-Yno-adapted-args",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Ywarn-value-discard",
  "-Ywarn-dead-code",
  "-Ywarn-unused"
)

//The default SBT testing java options are too small to support running many of the tests
// due to the need to launch Spark in local mode.
fork := true
javaOptions ++= Seq("-Xms512M", "-Xmx2G",  "-XX:+CMSClassUnloadingEnabled", "-XX:+UseG1GC")
parallelExecution in Test := false

lazy val spark = "2.1.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % spark % "provided",
  "org.apache.spark" %% "spark-sql" % spark % "provided",
  "org.apache.spark" %% "spark-hive" % spark % "provided",
  "org.datasyslab" % "geospark" % "0.5.2",
  // to use magellan
  // git clone https://github.com/harsha2010/magellan && cd magellan/
  // sbt publishLocal
  "harsha2010" %% "magellan" % "1.0.5-SNAPSHOT",
  "com.holdenkarau" % "spark-testing-base_2.11" % s"${spark}_0.6.0" % "test"
)

run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in(Compile, run), runner in(Compile, run))

assemblyMergeStrategy in assembly := {
  case PathList("com", "esotericsoftware", xs@_*) => MergeStrategy.last
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case _ => MergeStrategy.first
}

test in assembly := {}

initialCommands in console :=
  """
    |import java.awt.Color
    |import java.io.File
    |
    |
    |import java.io.File
    |
    |import org.apache.log4j.Logger
    |import org.apache.spark.SparkConf
    |import org.apache.spark.sql.SparkSession
    |import org.apache.spark.storage.StorageLevel
    |import org.datasyslab.geospark.enums.{ GridType, IndexType }
    |import org.datasyslab.geospark.spatialOperator.JoinQuery
    |import org.datasyslab.geospark.spatialRDD.{ PointRDD, PolygonRDD }
    |val conf: SparkConf = new SparkConf()
    |    .setAppName("geosparkProblems")
    |    .setMaster("local[*]")
    |    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    |
    |val spark: SparkSession = SparkSession
    |    .builder()
    |    .config(conf)
    |    //      .enableHiveSupport()
    |    .getOrCreate()
    |
    |@transient lazy val logger: Logger = Logger.getLogger(this.getClass)
    |import spark.implicits._
    |
    |  val path = "src" + File.separator + "main" + File.separator + "resources" + File.separator
    |  val pointsPath = path + "points.csv"
    |  val polygonPath = path + "polygon_multipolygon.csv"
  """.stripMargin

mainClass := Some("problemGeospark.GeoSpark")