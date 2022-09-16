plugins {
    id("scala")
    id("cz.alenkacz.gradle.scalafmt") version "1.16.2"
    id("com.github.maiflai.scalatest") version "0.32"
    id("maven-publish")
}

group = "com.mb.io"
version = "0.0.3"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.scala-lang:scala-library:2.12.16")

    implementation("org.apache.spark:spark-core_2.12:3.0.2")
    implementation("org.apache.spark:spark-sql_2.12:3.0.2")
    implementation("org.apache.spark:spark-hive_2.12:3.0.2")

    implementation("io.circe:circe-generic_2.12:0.9.3")
    implementation("io.circe:circe-parser_2.12:0.9.3")
    testImplementation("org.pegdown:pegdown:1.6.0")
    testImplementation("org.scalatest:scalatest_2.12:3.0.5")
}

java {
    withSourcesJar()
    withJavadocJar()
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            groupId = "com.mb.io"
            artifactId = "spark-writer"
            version = "0.0.3"
            from(components["java"])
        }
    }
}

