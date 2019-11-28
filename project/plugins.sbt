resolvers += Classpaths.sbtPluginReleases

addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "3.8.1")

//addSbtPlugin("com.typesafe.sbt" % "sbt-pgp" % "2.0.0") see https://github.com/sbt/sbt-pgp#setup

addSbtPlugin("org.scoverage" %% "sbt-scoverage" % "1.6.1")

addSbtPlugin("org.scoverage" % "sbt-coveralls" % "1.2.7")
