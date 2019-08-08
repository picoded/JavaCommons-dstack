#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# echo "-----------------------------------"
# echo "java-formatter directory location: $DIR"
# echo "source code directory (to format): $1"
# echo "-----------------------------------"


# Given a java path, format it according to the defined format, in the background (fork)
# @param  java file path
function format_java_path () {
	java -server -XX:+OptimizeStringConcat -cp "$DIR/java-formatter-with-dependencies.jar" Formatter "$DIR/code-format.opts" "$1" > /dev/null &
}

# Format the full path (no file search)
# format_java_path "$1"

#
# Format by directory depths
#
DIR_LIST=$(find "$1" -mindepth 4 -maxdepth 4 -type d)
for SUB_PATH in $DIR_LIST
do
	#echo "$JAVA_FILE"
	format_java_path "$SUB_PATH"
done

#
# Format by file (worse performance)
#

# # Get the full java file list
# JAVA_FILE_LIST=$(find $1 -iname '*.java' -print)
# # For each lets loop it
# for JAVA_FILE in $JAVA_FILE_LIST
# do
# 	#echo "$JAVA_FILE"
# 	format_java_file "$JAVA_FILE"
# done

# Wait for the various background processes
wait