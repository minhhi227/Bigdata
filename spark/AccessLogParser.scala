

case class LogParser(
    ipAddress: String, 
    user: String, 
    frank: String, 
    dateTime: String, 
    method: String,
    endpoint: String, 
    protocol: String, 
    responseCode: Int, 
    contentSize: Long) {}

object LogParser {

  val PATTERN = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\d+)""".r
  
  def parseLog(log: String): LogParser = {
    val res = PATTERN.findFirstMatchIn(log)

    if (res.isEmpty) {
      //throw new RuntimeException("error cannot parse log!!! : " + log)
       LogParser("","","","","","","",0,0)
    }
    else{
      val d = res.get
    
      LogParser(d.group(1), d.group(2), d.group(3), d.group(4), d.group(5), d.group(6),
      d.group(7), d.group(8).toInt, d.group(9).toLong)
    }

    
  }

}