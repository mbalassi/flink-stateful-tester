# Flink Stateful Tester

Simple utility job to test Flink stateful high availability functionality. The job remembers and stores the initial 
startup time for each source via Flink state and keeps emitting this information per parallel instance
periodically. This makes for a straight-forward verification as this information is to be retained between restarts.

## Build

The project is built via Maven 3, I am using version 3.6.0 at the time of writing. 

```
mvn clean install
```

The resulting `target/flink-stateful-tester-*jar` has its entrypoint automatically set.

