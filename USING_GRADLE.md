# Using Gradle to Build the project

This project uses gradle to install external dependencies, build the project (jar files), and execute tests.

## Gradle Version

The version of gradle required is Gradle 4.6, or as specified in `/gradle/wrapper/gradle-wrapper.properties`.

## Run tasks using gradlew instead of gradle

Run `./gradlew` instead of the globally installed `gradle` on your machine to execute gradle tasks.

This ensures that the correct version of gradle is used, and also downloads the required version of gradle.

## Tasks

### Build buildJavaCommonsCore

Run `./gradlew buildJavaCommonsCore` to build the JavaCommonsCore module.

### Build the jar packages

Run `./gradlew jar` to build the project and package as a jar.

Run `./gradlew shadowJar` to build the project and package as a shadow jar (dependencies are shaded)

Run `./gradlew fatJar` to build the project and package as a fat jar (dependencies are included, and not shaded)

Run `./gradlew buildAll` to build all 3 jars (jar, shadow jar, and fat jar).

Run `./gradlew testAndBuildAll` to run full test suite and build all 3 jars (jar, shadow jar, and fat jar).

## Setting  up the project in IntelliJ
1. Right click "settings.gradle" and click "Import Gradle Project"
2. In the Gradle Tool Window, click the wrench, and then "Gradle Settings"
3. Check "Download external annotations for dependencies" - this will allow code hinting to work
4. Set "Use Gradle from" to "'gradle-wrapper.properties' file" to make sure it runs the correct version of gradle


