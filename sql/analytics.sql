-- FlightPulse - Airline Delay Analytics
-- Spark SQL Queries

-- 1. Overall flight summary
SELECT
    COUNT(*) AS total_flights,
    ROUND(AVG(departureDelay), 2) AS avg_departure_delay,
    ROUND(AVG(arrivalDelay), 2) AS avg_arrival_delay,
    SUM(cancelled) AS cancelled_flights,
    SUM(diverted) AS diverted_flights
FROM flights;


-- 2. Airline statistics
SELECT
    airline,
    COUNT(*) AS total_flights,
    ROUND(AVG(departureDelay), 2) AS avg_departure_delay,
    ROUND(AVG(arrivalDelay), 2) AS avg_arrival_delay,
    SUM(CASE WHEN arrivalDelay > 15 THEN 1 ELSE 0 END)
        AS delayed_flights,
    ROUND(
        SUM(CASE WHEN arrivalDelay > 15 THEN 1 ELSE 0 END)
        / COUNT(*) * 100,
        2
    ) AS delay_rate
FROM flights
GROUP BY airline
ORDER BY delay_rate DESC;


-- 3. Delay classification
SELECT
    CASE
        WHEN arrivalDelay <= 0 THEN 'ON_TIME'
        WHEN arrivalDelay <= 15 THEN 'MINOR'
        WHEN arrivalDelay <= 60 THEN 'MODERATE'
        ELSE 'SEVERE'
    END AS delay_category,
    COUNT(*) AS flight_count
FROM flights
GROUP BY
    CASE
        WHEN arrivalDelay <= 0 THEN 'ON_TIME'
        WHEN arrivalDelay <= 15 THEN 'MINOR'
        WHEN arrivalDelay <= 60 THEN 'MODERATE'
        ELSE 'SEVERE'
    END
ORDER BY flight_count DESC;


-- 4. Airport statistics
SELECT
    origin AS airport,
    COUNT(*) AS total_departures,
    ROUND(AVG(departureDelay), 2) AS avg_departure_delay,
    SUM(CASE WHEN departureDelay > 15 THEN 1 ELSE 0 END)
        AS delayed_departures
FROM flights
GROUP BY origin
ORDER BY avg_departure_delay DESC;


-- 5. Route statistics
SELECT
    origin,
    destination,
    CONCAT(origin, '-', destination) AS route,
    COUNT(*) AS total_flights,
    ROUND(AVG(departureDelay), 2) AS avg_departure_delay,
    ROUND(AVG(arrivalDelay), 2) AS avg_arrival_delay,
    SUM(CASE WHEN arrivalDelay > 15 THEN 1 ELSE 0 END)
        AS delayed_flights
FROM flights
GROUP BY origin, destination
ORDER BY avg_arrival_delay DESC;


-- 6. Monthly statistics
SELECT
    month,
    COUNT(*) AS total_flights,
    ROUND(AVG(departureDelay), 2) AS avg_departure_delay,
    ROUND(AVG(arrivalDelay), 2) AS avg_arrival_delay,
    SUM(cancelled) AS cancelled_flights,
    SUM(diverted) AS diverted_flights
FROM flights
GROUP BY month
ORDER BY month;


-- 7. Top 10 most delayed routes
SELECT
    CONCAT(origin, '-', destination) AS route,
    COUNT(*) AS total_flights,
    ROUND(AVG(arrivalDelay), 2) AS avg_arrival_delay
FROM flights
GROUP BY origin, destination
HAVING COUNT(*) >= 50
ORDER BY avg_arrival_delay DESC
LIMIT 10;


-- 8. Airline ranking using window function
SELECT
    airline,
    ROUND(AVG(arrivalDelay), 2) AS avg_arrival_delay,
    RANK() OVER (
        ORDER BY AVG(arrivalDelay) DESC
    ) AS delay_rank
FROM flights
GROUP BY airline
ORDER BY delay_rank;


-- 9. Route ranking within each origin airport
SELECT
    origin,
    destination,
    ROUND(AVG(arrivalDelay), 2) AS avg_arrival_delay,
    RANK() OVER (
        PARTITION BY origin
        ORDER BY AVG(arrivalDelay) DESC
    ) AS route_rank
FROM flights
GROUP BY origin, destination
ORDER BY origin, route_rank;
