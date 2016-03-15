package cn.com.gf.bdp


private[bdp] class SQLOperateException(message: String, cause: Exception)
    extends Exception(message,cause) {

  def this(message: String) = {
    this(message, null)
  }

}

