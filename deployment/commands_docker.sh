#!/bin/bash

export PARENT_PATH="/home/app"
export INPUT_CONSOLE="/data/consoles.csv"
export INPUT_RESULT="/data/result.csv"
export OUTPUT="/data/output/"

echo " ------------------"
echo "| Executing Jar... |"
echo " ------------------"
java -jar $PARENT_PATH/dataflow/target/dataflow-bundled-1.0.0.jar  \
--inputConsoleData=$PARENT_PATH$INPUT_CONSOLE \
--inputResultData=$PARENT_PATH$INPUT_RESULT \
--NTop=10 \
--outputData=$PARENT_PATH$OUTPUT

ls $OUTPUT
echo " -------------------------------------------- "
echo "| Transforming the data from avro to json... |"
echo " -------------------------------------------- "
java -jar $PARENT_PATH/deployment/avro-tools-1.9.1.jar tojson $PARENT_PATH$OUTPUT/-00000-of-00001.avro > data.json

if [ $1 == "--formatData" ]
then
   cat data.json | grep -Eo '"[^"]*" *(: *([0-9]*|"[^"]*")[^{}\["]*|,)?|[^"\]\[\}\{]*|\{|\},?|\[|\],?|[0-9 ]*,?' | awk '{if ($0 ~ /^[}\]]/ ) offset-=4; printf "%*c%s\n", offset, " ", $0; if ($0 ~ /^[{\[]/) offset+=4}'
else
  cat data.json
fi
