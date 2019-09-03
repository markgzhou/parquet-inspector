# parquet-inspector

Inspect, check and validate parquet files

## Build
./gradlew clean shadowJar

## Run

#### Commands
cat, meta, schema

#### Usage
java -jar [parquet-inspector-all.jar] [command] [file]

#### Example
java -jar ./build/libs/parquet-inspector-all.jar schema /Data/parquet_file.parquet

### Example of schema comparison
In the examples folder, there is a `users.parquet` and two `.schema` files. The `same.schema` contains the schema description from `users.parquet`. `different.schema` contains a different schema description.

Generate same.schema:
```
java -jar parquet-inspector-all.jar schema users.parquet > same.schema
```

Compare schemas from parquet and contract schema file:
~~~~
java -jar parquet-inspector-all.jar schema users.parquet | diff same.schema -
java -jar parquet-inspector-all.jar schema users.parquet | diff different.schema -
~~~~ 

