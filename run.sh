rm -r output/
javac -classpath /Users/Mike/hadoop-1.2.1/hadoop-core-1.2.1.jar:classes -d classes/ *.java
jar -cvf main.jar -C classes/ .
hadoop jar main.jar OnoAverage $1 output/
cat output/part-r-00000