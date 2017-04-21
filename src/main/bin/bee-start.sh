#!/bin/sh

BEE_HOME=$BEE_HOME
LIBS=
command=

if [ $# != 1 ]; then
  echo ''
  echo 'palease inpurt start or stop param!'
  echo ''
  exit 1
else
  command=$1
fi


if [ -z $BEE_HOME ]; then
  echo 'Must be set BEE_HOME.'
  exit 2
fi

for lib in `ls $BEE_HOME/libs`
do
  LIBS=${LIBS}:$BEE_HOME/libs/$lib
done

export CLASS_PATH=$CLASS_PATH:$LIBS


if [ $command = "start" ]; then
  nohup java -jar $BEE_HOME/bee-1.0.jar com.bee.Bee &
elif [ $command = "stop" ]; then
  jps |grep Bee |awk {'print $1'} |xargs kill -9
else
  echo 'example : ./bee.sh start or ./bee.sh stop '
  exit 3
fi
