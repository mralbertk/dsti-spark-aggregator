case class CovidData(
  date: String, 
  state: String, 
  name: String, 
  code: String, 
  cases: String, 
  deaths: String
)

object CovidData {

  val R = """^(?<date>[-0-9]*),(?<state>[A-Z]*),(?<name>[\\u00c4-\\u00fcA-zÀ-ú'-.\\s]*),(?<code>[.0-9]*),(?<cases>[.0-9]*),(?<deaths>[.0-9]*)$""".r

  def fromString(s: String): TraversableOnce[CovidData] = s match {
    case R(
      date:String, 
      state:String, 
      name:String, 
      code:String, 
      cases:String, 
      deaths:String) => Some(CovidData(date, state, name, code, cases, deaths))
    case _ => None
  }

}
