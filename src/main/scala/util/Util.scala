package util
//En este singleton guardaremos todas las funciones auxiliares que tengamos que utilizar en la funcion principal

object Util {
  def mapDesco(str:Char): Char = {
    str match {
      case 'a' => 'o'
      case 'o' => 'a'
      case 'i' => 'u'
      case 'u' => 'i'
      case c => c
    }
  }

  def desencriptar(str:String): String = {
    val strTemp = str.map{
      case 'a' => 'o'
      case 'o' => 'a'
      case 'i' => 'u'
      case 'u' => 'i'
      case c => c}
    strTemp
  }

  def desencriptarForeach(str:String): String = {
    var strDesencriptado = ""
    str.foreach { c =>
      if(c == 'a') { strDesencriptado +='o'}
      else if (c == 'o') { strDesencriptado +='a'}
      else if (c == 'i') { strDesencriptado +='u'}
      else if (c == 'u') { strDesencriptado +='i'}
      else { strDesencriptado +=c}}
    strDesencriptado
  }
  def desencriptarMap(str:String): String = {
    var strDesencriptado = ""
    str.map(c =>
      if(c == 'a') { strDesencriptado +='o'}
      else if (c == 'o') { strDesencriptado +='a'}
      else if (c == 'i') { strDesencriptado +='u'}
      else if (c == 'u') { strDesencriptado +='i'}
      else { strDesencriptado +=c}
    )
    strDesencriptado
  }


  def desencriptarMatch(str:String): String = {
    var strDesencriptado = ""
    str.foreach { caract =>
      caract match {
        case 'a' => strDesencriptado +='o'
        case 'o' => strDesencriptado +='a'
        case 'i' => strDesencriptado +='u'
        case 'u' => strDesencriptado +='i'
        case _ => strDesencriptado +=caract
      }

    }
    strDesencriptado
  }
}
