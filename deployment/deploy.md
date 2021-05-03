#Summary

This pipeline take csv data as input and then transform it. 
Joining in first place the result data and consoles data. 
Then the games are grouped by Company and Console and gets the N top/worst games.
Also this pipeline obtains the N top/worst games for all consoles. 
Those 4 categories are grouped in a single record called "OutputRecord".
The output data is in avro format (check the schema in `de-challenge/schema/resources/OutputRecord.avsc`), 
but to show the data itself in `commands_docker.sh` or `commands_manual_run.sh` scripts 
the data is transformed as json using avro-tools jar.


#How to run it using Docker

You need already installed docker in your computer.
You have to run the following two commands:

##STEP 1
Go to "de-challenge" folder and run: 
```
docker build -t "de-challenge-efvr" .
```

##STEP 2
This command will show you  the data as json format, but not formatted
```
docker run de-challenge-efvr bash /home/app/deployment/commands_docker.sh
```
if you want see the json data formatted, you can add an extra parameter:
```
docker run de-challenge-efvr bash /home/app/deployment/commands_docker.sh --formatData
```

if you want modify the amount of the top/worst games, you can do it modifying the parameter `--NTop` in jar execution (Go to `commands_docker.sh` -> line 14).

#How to run it manually

##Prerequisites
 - Maven (3.6.3)
 - Java openjdk8

##Steps
1. Go to "deployment" folder and open the file `commands_manual_run.sh`.
2. Then replace the values in the four constants (PARENT_PATH, INPUT_CONSOLE, INPUT_RESULT, OUTPUT), remember the data is in "data" folder.
3. Run this bash script in your terminal:
```
bash commands_manual_run.sh
```

