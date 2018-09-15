INTIAL PART UNCHANGED FROM TEMPLATE

FOR SPECIFICS SEE DOWN

# Info

Version : Java 8

IDE : Intellij

OS : Windows 10

# Build

Compile and test
```
./gradlew build
```

Compile only
```
./gradlew assemble
```

Test
```
./gradlew test
```

For those who use Windows, you should run `gradlew.bat` with the same parameters.
Both IDEA and Eclipse have plugins for Gradle.
Some existing tests need Java 8.


# Code location

`src/main/java` is for word count code.

`src/test/java` is for tests. And `src/test/resources` is for test data.

final_result.txt also stores results of wordcount. (apart from the output stream we write to)

Intermediate files are serilaized and stored in 'src/tetxt/resources'

# Directions
Try wordcount.main() to run the predefined case.

Wordcount works for using the main() layout for any number of workers/ files.

# project-1-fall-2017-distributed-wordcount-AnishPimpley
project-1-fall-2017-distributed-wordcount-AnishPimpley created by GitHub Classroom

# Program Work Flow:

![flow images](https://github.com/AnishPimpley/Word_Count_with_MapReduce/blob/master/master_flow.jpg).

Finer details explained inline in code
