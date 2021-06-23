name := "AssignmentPart2"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.5"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.4.5"
libraryDependencies += "org.apache.bahir" %% "spark-streaming-twitter" % "2.3.4"
libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "3.5.1"
libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "3.5.1" classifier "models"