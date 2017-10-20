package com.dt.spark.main.BasicLearn

//import net.sf.json.JSONObject
import com.alibaba.fastjson.{JSONArray, JSONObject}

/**
  * Created by hjw on 16/7/17.
  */
case class eventInfo(
                      val eventId: Int,
                      val reqTime: Long
                    )

object WordCnt2 {
  def main(args: Array[String]) {


    var list = List(("123#12",12,List(eventInfo(123,12),eventInfo(124,22))),("223#12",12,List(eventInfo(123,12),eventInfo(124,22))))


    val pathDetailList = new JSONObject()
    pathDetailList.put("total", 12)

    val listInfoOuter =  new JSONArray()
    list.foreach(a => {
      val pathDetailElem = new JSONObject()
      pathDetailElem.put("path", a._1)
      pathDetailElem.put("total", a._2)

      val listInfo = new JSONArray()

      a._3.foreach(iter => {
        val pathDetailElem3 = new JSONObject()
        pathDetailElem3.put("eventId", iter.eventId)
        pathDetailElem3.put("time", iter.reqTime)
        listInfo.add(pathDetailElem3)
      })
      pathDetailElem.put("eventList", listInfo)
      listInfoOuter.add(pathDetailElem)
    })

    pathDetailList.put("detail", listInfoOuter)
    print(pathDetailList.toJSONString)

    print(1 until  12)


  }
}
