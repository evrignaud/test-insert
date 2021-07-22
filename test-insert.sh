#!/usr/bin/sh

export SCRIPTPATH="$( cd "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"

"$JAVA_HOME/bin/java" -jar $SCRIPTPATH/build/libs/insert-0.0.1-SNAPSHOT.jar 2000000 50

"$JAVA_HOME/bin/java" -jar $SCRIPTPATH/build/libs/insert-0.0.1-SNAPSHOT.jar 2000000 50 create_data_file_with_id

"$JAVA_HOME/bin/java" -jar $SCRIPTPATH/build/libs/insert-0.0.1-SNAPSHOT.jar 2000000 50 add_id_using_spark
