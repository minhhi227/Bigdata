echo "***************Process is started*****************"
echo "=========Prepare Environment======================"
hadoop fs -rm -r /user/cloudera/input
hadoop fs -mkdir -p /user/cloudera/input
hadoop fs -rm -r /user/cloudera/output
echo "=========Put input file to HDFS==================="
hadoop fs -put input.txt /user/cloudera/input/
echo "=========Run MapReduce Job========================"
hadoop jar wordcount.jar org.myorg.WordCount /user/cloudera/input /user/cloudera/output
echo "=========Output==================================="
hadoop fs -cat  /user/cloudera/output/* 
echo "***************Process is completed***************"
