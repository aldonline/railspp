import sbt._
import Keys._
import play.Project._

object ApplicationBuild extends Build {

  val appName         = "railspp"
  val appVersion      = "1.0-SNAPSHOT"

  val appDependencies = Seq(
    jdbc,
    anorm,
    "postgresql" % "postgresql" % "9.1-901-1.jdbc4",
    "org.openrdf.sesame" % "sesame-runtime" % "2.7.7",
    "com.googlecode.batchfb" % "batchfb" % "2.1.3"
  )


  val main = play.Project(appName, appVersion, appDependencies).settings(     
  )

}
