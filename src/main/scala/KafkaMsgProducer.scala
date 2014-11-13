package bigdata

import java.text.{DateFormat, SimpleDateFormat}
import java.util.{Calendar, Properties}

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}

import scala.util.Random


object KafkaMsgProducer {
  val broker = "10.214.208.14:9092"
  var ts = Array[String]()
  var te = Array[String]()

  def main(args: Array[String]) {

    if (args.length < 1) {
      System.err.println("Usage: KafkaMsgProducer <zkQuorum> ")
      System.exit(1)
    }

    val Array(zkQuorum) = args

    val props = new Properties()
    props.put("metadata.broker.list", zkQuorum)
    props.put("serializer.class", "kafka.serializer.StringEncoder")

    val config = new ProducerConfig(props)
    val producer = new Producer[String, String](config)

    val numlist = genD7D(producer)
    Thread.sleep(10000)

    gen24H(producer, numlist)

  }


  // 展示port 0100169
  // 生成一组FTVD7D
  def genD7D(p: Producer[String, String]): List[Int] = {

    val rand: Random = new Random()
    val st1: Calendar = Calendar.getInstance()
    st1.set(2014, 9, 10, 0, 0)
    val st2: Calendar = Calendar.getInstance()
    st2.set(2014, 9, 10, 0, 0)
    val st3: Calendar = Calendar.getInstance()
    st3.set(2014, 9, 10, 0, 0)
    val df: DateFormat = new SimpleDateFormat()
    val port = "0100169"
    val berth = Array("0190220", "0190221", "0190222")
    var time = Array(st1, st2, st3)
    val format = new SimpleDateFormat("yyyyMMddHHmm")
    val numlist = (100 to 400).map{ i =>
      // 选择泊位
      val num = rand.nextInt(3)

      // 生成入港时间,比上一个时间点至少迟2小时，最多8小时
      val h = rand.nextInt(5) + 2
      val m = rand.nextInt(60)
      time(num).add(Calendar.MINUTE, m)
      time(num).add(Calendar.HOUR, h)
      val date1 = format.format(time(num).getTime)



      // 生成出港时间，比入港时间至少迟4个小时，最多9消失
      val h2 = rand.nextInt(4) + 4
      val m2 = rand.nextInt(60)
      time(num).add(Calendar.MINUTE, m2)
      time(num).add(Calendar.HOUR, h2)
      val date2 = format.format(time(num).getTime)

      ts = ts :+ date1
      te = te :+ date2

      val name = s"威廉莎士比亚($i)号"

      val re: String = s"00:FTVD7D::9::::'10:::$name:$i::1:1:::::$date1::$port::::99'"
      p.send(new KeyedMessage[String, String]("message", re))
      println(re)


      val re2: String = s"00:FTVD7D::9::::'10:::$name:$i::1:2::::::$date2:$port::::99'"
      p.send(new KeyedMessage[String, String]("message", re2))
      println(re2)
//      Thread.sleep(100)

      num
    }.toList

    numlist
  }

  def gen24H(p: Producer[String, String], nl: List[Int]) = {
    val port = "0100169"
    val format = new SimpleDateFormat("yyyyMMddHHmm")


    var st: Calendar = Calendar.getInstance()
    val berlist = List("0190220", "0190221", "0190222")

    // 1. 根据船舶获取入港出港时间，以此基础生成24H报文的入港出港时间
    // 2. 在不冲突的船舶时间前提下，码头泊位随机生成，0，1，2

    (100 to 400).map{ i =>
      // 选择泊位
      val num = berlist(nl(i-100))


      val t1 = ts(i-100)
      val t2 = te(i-100)


      val name = s"威廉莎士比亚($i)号"

      val tp1 = s"00:VDL24H::9::::'10::::$name:$i::1:1:1:::::$t1::$num::::99'"
      p.send(new KeyedMessage[String, String]("message", tp1))
      println(tp1)

      val tp2 = s"00:VDL24H::9::::'10::::$name:$i::1:1:2::::::$t2:$num::::99'"
      p.send(new KeyedMessage[String, String]("message", tp2))
      println(tp2)

      Thread.sleep(1000)

    }

  }



}
