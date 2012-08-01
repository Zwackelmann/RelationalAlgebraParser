import sbt._
import Keys._

object RalalgBuild extends Build {
    lazy val realalg = Project(id = "relalg", base = file("."))
}
