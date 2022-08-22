case class CovidDataByCity(
  date: String, 
  state: String, 
  name: String, 
  code: String, 
  cases: String, 
  deaths: String
)

case class CovidDataByRegion(
  date: String,
  region: String, 
  state: String, 
  cases: String, 
  deaths: String
)

case class CovidDataByState(
  date: String,
  state: String, 
  cases: String, 
  deaths: String
)

object CovidDataByCity {

  val R = """^(?<date>[-0-9]*),(?<state>[A-Z]*),(?<name>[-\\u00c4-\\u00fcA-zÀ-ú'ÄÖÜäöü\s]*),(?<code>[.0-9]*),(?<cases>[.0-9]*),(?<deaths>[.0-9]*)$""".r

  def fromString(s: String): TraversableOnce[CovidDataByCity] = s match {
    case R(
      date:String, 
      state:String, 
      name:String, 
      code:String, 
      cases:String, 
      deaths:String) => Some(CovidDataByCity(date, state, name, code, cases, deaths))
    case _ => None
  }

}

object CovidDataByRegion {

  val R = """^(?<date>[-0-9]*),(?<region>[-A-z]*),(?<state>[A-Z]*),(?<cases>[.0-9]*),(?<deaths>[.0-9]*)$""".r

  def fromString(s: String): TraversableOnce[CovidDataByRegion] = s match {
    case R(
      date:String,
      region: String, 
      state:String,  
      cases:String, 
      deaths:String) => Some(CovidDataByRegion(date, region, state, cases, deaths))
    case _ => None
  }

}

object CovidDataByState {

  val R = """^(?<date>[-0-9]*),(?<state>[A-Z]*),(?<cases>[.0-9]*),(?<deaths>[.0-9]*)$""".r

  def fromString(s: String): TraversableOnce[CovidDataByState] = s match {
    case R(
      date:String,
      state:String,  
      cases:String, 
      deaths:String) => Some(CovidDataByState(date, state, cases, deaths))
    case _ => None
  }

}

