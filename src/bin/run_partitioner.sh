#!/bin/bash
EXIT_KO=1
EXIT_OK=0

####################### helper common functions  ##############################
# print current time formatted
function getcurrentdatetime() {
  date
}
# input parameters
# $1 : log message
function loginfo() {
    local logmessage=$1
    printf '%s [%-4s] %s\n' "$(getcurrentdatetime)" "info" "$logmessage"
}

# input parameters
# $1 : log message
function logwarning() {
    local logmessage=$1
    printf '%-8s %s\n' "warning" "$logmessage"
}

# input parameters
# $1 : log message
function logerror(){
    local logmessage=$1
    printf '%s [%-5s] %s\n' "$(getcurrentdatetime)" "error" "$logmessage"
}

# display the error message and exit
function exitonerror() {
    echo
    logerror "$1"
    if [ "$2" != "" ]
    then
        print_help
    fi
    echo "procedure aborted."
    echo
    exit $EXIT_KO
}

function print_help() {
        echo
        echo "This script is used to execute the partitioner of LAWA."
        echo "the following parameters are mandatory:"

        echo "  -m                   : partitioner mode. 0 for LAP and 1 for BLAP."
        echo "  -p                   : number of partitions. The number o partitions that the given dataset should be devided into."
        echo "  -n                   : dataset name. name of the dataset"
        echo "  -i                   : hdfs path of dataset instances."
        echo "  -s                   : hdfs path of dataset schema."
        echo "  -j                   : path of jar. The path of the jar to be used for spark-submit"

    echo "example: $0 -m 0 -p 4 -n my_dataset -b hdfs:///localhost:9000 -i hdfs:///localhost:9000/my_dataset/instances \\
    -s hdfs:///localhost:9000/my_dataset/schema -j /home/LAWA-SNAPSHOT-1.0.jar"
}

function parsearg() {
    if [ "$#" -eq 0 ]; then
        exitonerror "missing arguments !" "help"
    fi
    while getopts ":hm:p:n:b:i:s:j:" option
    do
                case $option in
                        h)
                        print_help
                        exit $EXIT_OK
                        ;;
                        m)
                        PARTITION_MODE=$OPTARG
                        ;;
                        p)
                        NUM_PARTITIONS=$OPTARG
                        ;;
                        n)
                        DATASET_NAME=$OPTARG
                        ;;
                        b)
                        HDFS_BASE_PATH=$OPTARG
                        ;;
                        i)
                        INSTANCES_PATH=$OPTARG
                        ;;
                        s)
                        SCHEMA_PATH=$OPTARG
                        ;;
                        j)
                        JAR_PATH=$OPTARG
                        ;;
                        \?)
                        exitonerror "unexpected option -$OPTARG " "help"
                        ;;
        esac
    done
}


function spark_submit() {
  spark-submit \
  --class org.ics.isl.partitioner.PartitionerMain \
  --driver-memory 1G \
  --executor-memory 1G \
  --conf spark.speculation=true \
  --master local[2] \
  $JAR_PATH -m $PARTITION_MODE -p $NUM_PARTITIONS -d $DATASET_NAME -h $HDFS_BASE_PATH -i $INSTANCES_PATH -s $SCHEMA_PATH
}


parsearg "$@"
spark_submit