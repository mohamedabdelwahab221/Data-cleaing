
CREATE TABLE TemperaturesByCity (
    dt DATE NOT NULL,
    AverageTemperature DOUBLE NULL,
    AverageTemperatureUncertainty DOUBLE NULL,
    City VARCHAR(255) NOT NULL,
    Country VARCHAR(255) NOT NULL,
    Latitude VARCHAR(255) NULL,
    Longitude VARCHAR(255) NULL
);

-- Import data from CSV into the TemperaturesByCountry table
LOAD DATA INFILE 'E:\\climate\\GlobalLandTemperaturesByCity.csv'
INTO TABLE temperaturesbycity
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

-- Explore data to verify the import i wanted to check this line because when I was importing it had some errors
SELECT country
FROM TemperaturesByCountry
WHERE dt = '1824-01-01' AND AverageTemperature = 25.575;

-- Disable safe mode to allow data updates
SET SQL_SAFE_UPDATES = 0;

-- Correct corrupted data in the Country column
UPDATE TemperaturesByCountry
SET Country = 'Bonaire, Saint Eustatius And Saba'
WHERE dt = '1824-01-01' AND AverageTemperature = 25.575;

-- Explore unique country values in the data
SELECT DISTINCT country
FROM TemperaturesByCountry;

-- Since searching for Egypt didn't work, check for Cairo in the TemperaturesByCity table
SELECT *
FROM TemperaturesByCity;

-- Search for records where the Country column contains 'gypt'
SELECT *
FROM TemperaturesByCity
WHERE Country LIKE '%gypt%';

-- Create a temporary table for Egypt to optimize queries
CREATE TEMPORARY TABLE egypt AS
SELECT *
FROM TemperaturesByCity
WHERE Country = 'Egypt';

-- Check distinct cities in the temporary table
SELECT DISTINCT city
FROM egypt;

-- Check data for Cairo in 1900
SELECT *
FROM egypt
WHERE city = 'Cairo' AND YEAR(dt) = 1900
ORDER BY dt DESC;

-- Identify duplicates in the TemperaturesByCity table
WITH cte_temp AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY dt, AverageTemperature, AverageTemperatureUncertainty, City, Country ORDER BY dt ASC) AS dup
    FROM TemperaturesByCity
)
SELECT *
FROM cte_temp;

-- Create a copy of the TemperaturesByCity table to handle duplicates becasue we cant update it
CREATE TABLE TemperaturesByCity2 (
    dt DATE NOT NULL,
    AverageTemperature DOUBLE NULL,
    AverageTemperatureUncertainty DOUBLE NULL,
    City VARCHAR(255) NOT NULL,
    Country VARCHAR(255) NOT NULL,
    Latitude VARCHAR(255) NULL,
    Longitude VARCHAR(255) NULL,
    dup int
);

INSERT INTO temperaturesbycity2
WITH cte_temp AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY dt, AverageTemperature, AverageTemperatureUncertainty, City, Country ORDER BY dt ASC) AS dup
    FROM TemperaturesByCity
)
SELECT *
FROM cte_temp;

-- Check for remaining duplicates in the new table
SELECT *
FROM temperaturesbycity2
WHERE dup > 1;

-- Delete duplicates rows
DELETE FROM temperaturesbycity2
WHERE dup > 1;

-- DELETE column dup
ALTER TABLE temperaturesbycity2
DROP COLUMN dup;


-- Check for null values in the new table
SELECT *
FROM TemperaturesByCity2
WHERE AverageTemperature IS NULL;

-- Delete rows with null values in AverageTemperature Because its useless to our analyze that is based on AverageTemperature
DELETE FROM TemperaturesByCity2
WHERE AverageTemperature IS NULL;


-- Create a temporary table for Egypt with cleaned data
CREATE TEMPORARY TABLE egypt AS
SELECT *
FROM TemperaturesByCity2
WHERE Country = 'Egypt' or Country like '%gypt%';

-- Calculate the average summer temperature for Cairo after 1900
SELECT YEAR(dt) AS year, 
       city,
       AVG(AverageTemperature) AS average_temp_if_summer,
       AVG(AVG(AverageTemperature)) OVER (PARTITION BY city ORDER BY YEAR(dt)) AS rolling_avg
FROM egypt
WHERE  YEAR(dt) >= 1900 AND MONTH(dt) IN (6, 7, 8, 9)
GROUP BY YEAR(dt);

-- Calculate the average summer temperature for all cities in Egypt after 1900
SELECT 
    YEAR(dt) AS year, 
    city,
    AVG(AverageTemperature) AS average_temp_if_summer,
    AVG(AVG(AverageTemperature)) 
        OVER (PARTITION BY city 
              ORDER BY YEAR(dt)) AS rolling_avg_temp
FROM egypt
WHERE YEAR(dt) >= 1900 
  AND MONTH(dt) IN (6, 7, 8, 9)
GROUP BY YEAR(dt), city;


-- Create a view to visualize summer temperatures in Egypt
CREATE VIEW temp_of_summer AS
SELECT 
    YEAR(dt) AS year, 
    city,
    AVG(AverageTemperature) AS average_temp_if_summer,
    AVG(AVG(AverageTemperature)) 
        OVER (PARTITION BY city 
              ORDER BY YEAR(dt)) AS rolling_avg_temp
FROM temperaturesbycity2
WHERE (Country = 'Egypt' or Country like '%gypt%') and YEAR(dt) >= 1900 
  AND MONTH(dt) IN (6, 7, 8, 9)
GROUP BY YEAR(dt), city;

-- Note: After visualizing the data in Power BI, it seems the dataset may be random or not real. 
-- The project provided valuable insights into data cleaning and importing processes.
