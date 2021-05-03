#!/bin/bash

export INPUT_CONSOLE="../data/consoles.csv"
export INPUT_RESULT="../data/result.csv"
export OUTPUT="output/"

echo "Executing Jar..."
java -jar ../dataflow/target/dataflow-bundled-1.0.0.jar \
--inputConsoleData=$INPUT_CONSOLE \
--inputResultData=$INPUT_RESULT \
--outputData=$OUTPUT

echo "Transforming the data from avro to json"
java -jar avro-tools-1.9.1.jar tojson $OUTPUT/-00000-of-00001.avro > data.json

cat data.json