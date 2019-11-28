name := "riak-scala-client"

version := "0.9.5-akka-http-SNAPSHOT"

scalaVersion := "2.12.10"

crossScalaVersions := Seq("2.11.8", "2.12.10")

crossVersion := CrossVersion.binary

organization := "com.scalapenos"

organizationHomepage := Some(url("http://scalapenos.com/"))

licenses := Seq("The Apache Software License, Version 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt"))

homepage := Some(url("http://riak.scalapenos.com"))

scalacOptions := Seq("-encoding", "utf8",
                     "-target:jvm-1.8",
                     "-Xexperimental", // single abstract method for 2.11.8
                     "-feature",
                     "-language:implicitConversions",
                     "-language:postfixOps",
                     "-unchecked",
                     "-deprecation",
                     "-Xlog-reflective-calls",
                     "-Ywarn-adapted-args"
                    )

resolvers ++= Seq("Scalaz Bintray Repo" at "http://dl.bintray.com/scalaz/releases")
resolvers ++= Seq("snapshots", "releases").map(Resolver.sonatypeRepo)

val akkaVersion = "2.5.26" // cannot use 2.6.0 because it doesn't publish artifacts for 2.11.8
val akkaHttpVersion = "10.1.10"
val specs2Version = "2.4.17"

libraryDependencies ++=
  Seq(
    "com.typesafe.akka"      %%  "akka-actor"           % akkaVersion,
    "com.typesafe.akka"      %%  "akka-stream"          % akkaVersion,
    "com.typesafe.akka"      %%  "akka-slf4j"           % akkaVersion,
    "com.typesafe.akka"      %%  "akka-http-core"       % akkaHttpVersion,
    "com.typesafe.akka"      %%  "akka-http"            % akkaHttpVersion,
    "com.typesafe.akka"      %%  "akka-http-spray-json" % akkaHttpVersion,
    "com.github.nscala-time" %%  "nscala-time"          % "2.22.0",
    "com.typesafe.akka"      %%  "akka-testkit"         % akkaVersion       % "test",
    "com.typesafe.akka"      %%  "akka-http-testkit"    % akkaHttpVersion   % "test",
    "org.specs2"             %%  "specs2"               % specs2Version     % "test",
    "ch.qos.logback"         %   "logback-classic"      % "1.1.2"           % "provided"
  )

initialCommands in console += {
  List("import com.scalapenos.riak._", "import akka.actor._").mkString("\n")
}
