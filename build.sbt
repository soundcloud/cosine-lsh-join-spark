organization := "com.soundcloud"

name := "cosine-lsh-join-spark"

version := "0.0.3"

scalaVersion := "2.10.4"

// do not run multiple SparkContext's in local mode in parallel
parallelExecution in Test := false

// main dependencies
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.4.1" % "provided",
  "org.apache.spark" %% "spark-mllib" % "1.4.1" % "provided"
)

// test dependencies
libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.2.0" % "test"
)

// maven settings
publishMavenStyle := true

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases" at nexus + "service/local/staging/deploy/maven2")
}

publishArtifact in Test := false


pomExtra := (
    <url>https://github.com/soundcloud/cosine-lsh-join-spark</url>
    <description>A spark library for approximate nearest neighbours (ANN).</description>
    <licenses>
      <license>
        <name>MIT License</name>
        <url>http://www.opensource.org/licenses/mit-license.php</url>
        <distribution>repo</distribution>
      </license>
    </licenses>
    <scm>
      <connection>scm:git:git@github.com:soundcloud/cosine-lsh-join-spark.git</connection>
      <url>git@github.com:soundcloud/cosine-lsh-join-spark.git</url>
      <developerConnection>scm:git:git@github.com:soundcloud/cosine-lsh-join-spark.git</developerConnection>
    </scm>
    <developers>
      <developer>
        <id>ozgurdemir</id>
        <name>Özgür Demir</name>
        <organization>SoundCloud</organization>
        <url>http://soundcloud.com</url>
      </developer>
    </developers>)