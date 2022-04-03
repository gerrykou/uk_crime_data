-- Query to produce the dashboard in Data Studio
SELECT
IFNULL(NULLIF(age_range,''),'N/A') AS age_range,
IFNULL(NULLIF(outcome,''),'N/A') AS outcome,
IFNULL(NULLIF(gender,''),'N/A') AS gender,
datetime,
IFNULL(NULLIF(officer_defined_ethnicity,''),'N/A') AS officer_defined_ethnicity,
IFNULL(NULLIF(type,''),'N/A') AS type,
IFNULL(NULLIF(object_of_search,''),'N/A') AS object_of_search,
latitude,
longitude,
lat_long
FROM 'de-bootcamp-339509.stop_and_search.stop_and_search_partitioned_table'   

-- Various Queries
-- Order 'object of search' with the highest Record Count
SELECT 
object_of_search, 
count(*) AS count 
FROM `de-bootcamp-339509.stop_and_search.stop_and_search_partitioned_table` 
GROUP BY object_of_search 
ORDER BY count DESC   

-- Order Year-Month with the highest Record Count
SELECT 
count(*) AS count,
CONCAT(EXTRACT(YEAR FROM date(datetime)), '-', EXTRACT(MONTH FROM date(datetime))) AS month 
FROM `de-bootcamp-339509.stop_and_search.stop_and_search_partitioned_table` 
GROUP BY month 
ORDER BY count DESC