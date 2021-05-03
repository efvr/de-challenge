#!/bin/bash

export PARENT_PATH = "<replace-parent-path>/de-challenge"
export INPUT_CONSOLE="<replace-this>"
export INPUT_RESULT="<replace-this>"
export OUTPUT="<replace-this>"

echo " -----------------"
echo "| Building Jar... |"
echo " -----------------"
cd $PARENT_PATH
mvn clean package

echo " ------------------"
echo "| Executing Jar... |"
echo " ------------------"
java -jar $PARENT_PATH/dataflow/target/dataflow-bundled-1.0.0.jar  \
--inputConsoleData=$PARENT_PATH$INPUT_CONSOLE \
--inputResultData=$PARENT_PATH$INPUT_RESULT \
--outputData=$OUTPUT

ls $OUTPUT
echo " -------------------------------------------- "
echo "| Transforming the data from avro to json... |"
echo " -------------------------------------------- "
java -jar /de-challenge/deployment/avro-tools-1.9.1.jar tojson $OUTPUT/-00000-of-00001.avro > data.json

if [ $1 == "--formatData" ]
then
   cat data.json | grep -Eo '"[^"]*" *(: *([0-9]*|"[^"]*")[^{}\["]*|,)?|[^"\]\[\}\{]*|\{|\},?|\[|\],?|[0-9 ]*,?' | awk '{if ($0 ~ /^[}\]]/ ) offset-=4; printf "%*c%s\n", offset, " ", $0; if ($0 ~ /^[{\[]/) offset+=4}'
else
  cat data.json
fi
