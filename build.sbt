import sbtassembly.AssemblyPlugin.defaultShellScript

lazy val commonSettings = Seq(
  version := "0.0.4",
  scalaVersion := "2.11.8"
)

lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    name := "Profiler"
  )

libraryDependencies += "org.scala-lang.modules" % "scala-parser-combinators_2.11" % "1.0.4"

assemblyOption in assembly := (assemblyOption in assembly).value.copy(prependShellScript = Some(defaultShellScript))

assemblyJarName in assembly := s"${name.value}-${version.value}"
