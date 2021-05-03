#!/bin/bash

export INPUT_CONSOLE="/home/app/data/consoles.csv"
export INPUT_RESULT="/home/app/data/result.csv"
export OUTPUT="/home/app/data/output/"

echo "Executing Jar..."
java -jar /home/app/dataflow/target/dataflow-bundled-1.0.0.jar  \
--inputConsoleData=$INPUT_CONSOLE \
--inputResultData=$INPUT_RESULT \
--outputData=$OUTPUT

ls $OUTPUT
echo "\nTransforming the data from avro to json"
java -jar /home/app/Deployment/avro-tools-1.9.1.jar tojson $OUTPUT/-00000-of-00001.avro > data.json

cat data.json | grep -Eo '"[^"]*" *(: *([0-9]*|"[^"]*")[^{}\["]*|,)?|[^"\]\[\}\{]*|\{|\},?|\[|\],?|[0-9 ]*,?' | awk '{if ($0 ~ /^[}\]]/ ) offset-=4; printf "%*c%s\n", offset, " ", $0; if ($0 ~ /^[{\[]/) offset+=4}'
