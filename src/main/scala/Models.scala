case class Flight(
  year: Int,
  month: Int,
  day: Int,
  dayOfWeek: Int,
  airline: String,
  flightNumber: String,
  origin: String,
  destination: String,
  departureDelay: Double,
  arrivalDelay: Double,
  cancelled: Int,
  diverted: Int
)

case class Airline(
  code: String,
  name: String
)

case class Airport(
  code: String,
  name: String,
  city: String,
  state: String,
  country: String
)
