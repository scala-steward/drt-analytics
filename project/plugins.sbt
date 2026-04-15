
ThisBuild / libraryDependencySchemes ++= Seq(
  "org.scala-lang.modules" % "scala-xml" % VersionScheme.Always
)

addSbtPlugin("com.github.sbt"         % "sbt-native-packager"   % "1.11.7")
addSbtPlugin("org.scoverage"          % "sbt-scoverage"         % "2.4.4")
addSbtPlugin("org.johnnei.scapegoat" %% "sbt-scapegoat"         % "1.3.7")
addSbtPlugin("com.eed3si9n"           % "sbt-assembly"          % "2.3.1")
addSbtPlugin("net.nmoncho"            % "sbt-dependency-check"  % "1.8.4")
addSbtPlugin("com.timushev.sbt"       % "sbt-updates"           % "0.6.3")
addDependencyTreePlugin
