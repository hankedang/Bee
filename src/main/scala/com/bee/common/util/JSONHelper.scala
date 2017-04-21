package com.bee.common.util

import net.sf.json.{JSONArray, JSONObject}

/**
  * Created by han.kedang on 16/9/14.
  */


class JOHelper {
  private val obj = new JSONObject

  def getJSONObject = obj

  def put(key: Any, value: Any): JOHelper = {
    obj.put(key, value)
    this
  }

}


class JAHelper {
  private val arr = new JSONArray

  def getJSONArray = arr

  def add(any: Any): JAHelper = {
    arr.add(any)
    this
  }

}
