import csv
import random
import os
from datetime import date, timedelta

random.seed(42)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

INPUT_DIR = os.path.join(BASE_DIR, "data", "input")
REFERENCE_DIR = os.path.join(BASE_DIR, "data", "reference")

os.makedirs(INPUT_DIR, exist_ok=True)
os.makedirs(REFERENCE_DIR, exist_ok=True)


# ============================================================
# AIRLINE REFERENCE DATA
# ============================================================

airlines = [
    ("AA", "American Airlines"),
    ("DL", "Delta Air Lines"),
    ("UA", "United Airlines"),
    ("WN", "Southwest Airlines"),
    ("B6", "JetBlue Airways"),
    ("AS", "Alaska Airlines"),
    ("NK", "Spirit Airlines"),
    ("F9", "Frontier Airlines")
]


with open(
    os.path.join(REFERENCE_DIR, "airlines.csv"),
    "w",
    newline=""
) as file:

    writer = csv.writer(file)

    writer.writerow([
        "code",
        "name"
    ])

    writer.writerows(airlines)


# ============================================================
# AIRPORT REFERENCE DATA
# ============================================================

airports = [
    ("ATL", "Hartsfield-Jackson Atlanta International", "Atlanta", "GA", "USA"),
    ("DFW", "Dallas/Fort Worth International", "Dallas", "TX", "USA"),
    ("DEN", "Denver International", "Denver", "CO", "USA"),
    ("ORD", "Chicago O'Hare International", "Chicago", "IL", "USA"),
    ("LAX", "Los Angeles International", "Los Angeles", "CA", "USA"),
    ("JFK", "John F. Kennedy International", "New York", "NY", "USA"),
    ("BOS", "Boston Logan International", "Boston", "MA", "USA"),
    ("SFO", "San Francisco International", "San Francisco", "CA", "USA"),
    ("SEA", "Seattle-Tacoma International", "Seattle", "WA", "USA"),
    ("MIA", "Miami International", "Miami", "FL", "USA"),
    ("PHX", "Phoenix Sky Harbor International", "Phoenix", "AZ", "USA"),
    ("LAS", "Harry Reid International", "Las Vegas", "NV", "USA"),
    ("MSP", "Minneapolis-Saint Paul International", "Minneapolis", "MN", "USA"),
    ("DTW", "Detroit Metropolitan Airport", "Detroit", "MI", "USA"),
    ("CLT", "Charlotte Douglas International", "Charlotte", "NC", "USA"),
    ("IAH", "George Bush Intercontinental", "Houston", "TX", "USA")
]


with open(
    os.path.join(REFERENCE_DIR, "airports.csv"),
    "w",
    newline=""
) as file:

    writer = csv.writer(file)

    writer.writerow([
        "code",
        "name",
        "city",
        "state",
        "country"
    ])

    writer.writerows(airports)


# ============================================================
# FLIGHT DATA
# ============================================================

airport_codes = [airport[0] for airport in airports]

num_records = 50000

output_file = os.path.join(
    INPUT_DIR,
    "flights.csv"
)


with open(
    output_file,
    "w",
    newline=""
) as file:

    writer = csv.writer(file)

    writer.writerow([
        "year",
        "month",
        "day",
        "dayOfWeek",
        "airline",
        "flightNumber",
        "origin",
        "destination",
        "departureDelay",
        "arrivalDelay",
        "cancelled",
        "diverted"
    ])

    start_date = date(2015, 1, 1)

    for i in range(1, num_records + 1):

        flight_date = start_date + timedelta(
            days=random.randint(0, 364)
        )

        airline = random.choice(airlines)[0]

        origin = random.choice(airport_codes)

        destination = random.choice(airport_codes)

        while destination == origin:
            destination = random.choice(airport_codes)

        # Most flights have small delays,
        # while a smaller percentage have severe delays.
        delay_type = random.random()

        if delay_type < 0.60:
            departure_delay = random.gauss(5, 8)

        elif delay_type < 0.85:
            departure_delay = random.gauss(25, 12)

        elif delay_type < 0.97:
            departure_delay = random.gauss(60, 25)

        else:
            departure_delay = random.gauss(120, 40)

        departure_delay = round(
            max(-20, departure_delay),
            2
        )

        # Arrival delay is correlated with departure delay.
        arrival_delay = (
            departure_delay
            + random.gauss(3, 8)
        )

        arrival_delay = round(
            max(-30, arrival_delay),
            2
        )

        # Cancellation probability
        cancelled = 1 if random.random() < 0.025 else 0

        # Diverted probability
        diverted = 1 if random.random() < 0.008 else 0

        # Cancelled flights generally have no meaningful delay.
        if cancelled == 1:
            departure_delay = 0.0
            arrival_delay = 0.0

        writer.writerow([
            flight_date.year,
            flight_date.month,
            flight_date.day,
            flight_date.isoweekday(),
            airline,
            f"{airline}{1000 + (i % 9000)}",
            origin,
            destination,
            departure_delay,
            arrival_delay,
            cancelled,
            diverted
        ])


print("=" * 60)
print("FlightPulse Dataset Generated Successfully")
print("=" * 60)
print(f"Flight records : {num_records}")
print(f"Flight file    : {output_file}")
print("Reference data : airlines.csv")
print("Reference data : airports.csv")
print("=" * 60)
