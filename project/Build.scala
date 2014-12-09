import com.typesafe.sbt.SbtScalariform.scalariformSettings
import sbt._
import sbtassembly.Plugin._
import AssemblyKeys._
import Keys._

object PDFOnSparkBuild extends Build {

	lazy val baseSettings = Defaults.defaultSettings ++ scalariformSettings ++ assemblySettings  ++ Seq(  
    version := "0.1",
    organization := "us.marek",
    scalaVersion := "2.10.4",
    fork := true,
    fork in Test := false,
    javaOptions ++= Seq("-Xmx2G", "-Xms64M", "-XX:MaxPermSize=512M", "-XX:+UseConcMarkSweepGC", "-XX:+CMSClassUnloadingEnabled"),
    parallelExecution := true,
    parallelExecution in Test := false,
    pollInterval := 1000,
    resolvers ++= Seq("Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
                      "Maven Central" at "http://repo1.maven.org",
                      "Sonatype snapshots" at "http://oss.sonatype.org/content/repositories/snapshots/",
                      "Sonatype releases" at "https://oss.sonatype.org/content/repositories/releases/"
                     ),
    libraryDependencies ++= Seq("org.scalatest"     %  "scalatest_2.10"  %  "2.2.1" % "test",
    	                        "org.apache.pdfbox" %  "pdfbox"          %  "1.8.7",
    	                        "org.apache.spark" %%  "spark-assembly"  %  "1.1.1",
    	                        "org.apache.tika"   %  "tika-bundle"     %  "1.6"
                                ),
    javacOptions ++= Seq("-source", "1.6", "-target", "1.6"),
    scalacOptions ++= Seq("-unchecked", "-deprecation"),
    shellPrompt <<= name(name => { state: State =>
	  object devnull extends ProcessLogger {
		  def info(s: => String) {}
		  def error(s: => String) {}
		  def buffer[T](f: => T): T = f
	  }
	  val current = """\*\s+(\w+)""".r
	  def gitBranches = ("git branch --no-color" lines_! devnull mkString)
	    "%s:%s>" format (
	      name,
		  current findFirstMatchIn gitBranches map (_.group(1)) getOrElse "-"
  	    )
    })
  )

  lazy val root = project
    .in(file("."))
    .settings(baseSettings: _*)

}