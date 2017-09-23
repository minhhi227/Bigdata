echo "***************Process is started*****************"
echo "=========Prepare Environment======================"
hadoop fs -rm -r /user/cloudera/spark/input
hadoop fs -mkdir -p /user/cloudera/spark/input
echo "=========Put input file to HDFS==================="
hadoop fs -put access_log.txt /user/cloudera/spark/input/
echo "=========Run Spark================================"
STARTTIME=`date +%s.%N`
spark-submit --class "ApacheLogFileAnalytics" myspark.jar "hdfs:///user/cloudera/spark/input/access_log.txt" > output.txt
ENDTIME=`date +%s.%N`
TIMEDIFF=`echo "$ENDTIME - $STARTTIME" | bc | awk -F"." '{print $1"."substr($2,1,3)}'`
echo "=========Output==================================="
cat output.txt 
echo "Execution Time : $TIMEDIFF"
echo "***************Process is completed***************"
