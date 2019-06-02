import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

val ktor_version: String by project
val kotlin_version: String by project
val logback_version: String by project

plugins {
    application
    kotlin("jvm") version "1.3.31"
}

group = "ktor-api-with-xodus"
version = "0.0.1"

application {
    mainClassName = "io.shmaks.samples.ktor.ApplicationKt"
}

repositories {
    mavenLocal()
    jcenter()
    maven { url = uri("https://kotlin.bintray.com/ktor") }
}

dependencies {
    compile("org.jetbrains.kotlin:kotlin-stdlib-jdk8:$kotlin_version")
    implementation("io.ktor:ktor-server-netty:$ktor_version")
    implementation("ch.qos.logback:logback-classic:$logback_version")
    implementation("io.ktor:ktor-gson:$ktor_version")
    implementation("de.nielsfalk.ktor:ktor-swagger:0.5.0")
    implementation(group = "com.github.ajalt", name = "clikt", version = "1.3.0")

//    compile("org.jetbrains.xodus:xodus-openAPI:1.3.0")
//    compile("org.jetbrains.xodus:xodus-entity-store:1.3.0")
//    compile("org.jetbrains.xodus:dnq:1.3.440")

    implementation("org.jetbrains.exposed:exposed:0.13.7")
    implementation("com.h2database:h2:1.4.197")

    implementation("org.koin:koin-ktor:2.0.1")

    testCompile("io.ktor:ktor-server-tests:$ktor_version")
}

kotlin.sourceSets["main"].kotlin.srcDirs("src")
kotlin.sourceSets["test"].kotlin.srcDirs("test")

sourceSets["main"].resources.srcDirs("resources")
sourceSets["test"].resources.srcDirs("testresources")

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "1.8"
}


