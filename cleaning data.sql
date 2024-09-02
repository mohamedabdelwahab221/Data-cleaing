CREATE TABLE TemperaturesByCity (
    dt DATE NOT NULL,
    AverageTemperature DOUBLE NULL,
    AverageTemperatureUncertainty DOUBLE NULL,
    City VARCHAR(255) NOT NULL,
    Country VARCHAR(255) NOT NULL,
    Latitude VARCHAR(255) NULL,
    Longitude VARCHAR(255) NULL
);

CREATE TABLE TemperaturesByCountry (
    dt DATE NOT NULL,
    AverageTemperature DOUBLE NULL,
    AverageTemperatureUncertainty DOUBLE NULL,
    Country VARCHAR(255) NOT NULL
);

CREATE TABLE TemperaturesByMajorCity (
    dt DATE NOT NULL,
    AverageTemperature DOUBLE NULL,
    AverageTemperatureUncertainty DOUBLE NULL,
    City VARCHAR(255) NOT NULL,
    Country VARCHAR(255) NOT NULL,
    Latitude VARCHAR(255) NULL,
    Longitude VARCHAR(255) NULL
);

CREATE TABLE TemperaturesByState (
    dt DATE NOT NULL,
    AverageTemperature DOUBLE NULL,
    AverageTemperatureUncertainty DOUBLE NULL,
    State VARCHAR(255) NOT NULL,
    Country VARCHAR(255) NOT NULL
);

CREATE TABLE GlobalTemperatures (
    dt DATE NOT NULL,
    LandAverageTemperature DOUBLE NULL,
    LandAverageTemperatureUncertainty DOUBLE NULL,
    LandMaxTemperature DOUBLE NULL,
    LandMaxTemperatureUncertainty DOUBLE NULL,
    LandMinTemperature DOUBLE NULL,
    LandMinTemperatureUncertainty DOUBLE NULL,
    LandAndOceanAverageTemperature DOUBLE NULL,
    LandAndOceanAverageTemperatureUncertainty DOUBLE NULL,
    PRIMARY KEY (dt)
);

-- Import data from CSV into the TemperaturesByCountry table
LOAD DATA INFILE 'E:\\climate change\\GlobalLandTemperaturesByCity.csv'
INTO TABLE TemperaturesByCountry
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

-- Create a distinct copy of the TemperaturesByCity table to handle duplicates becasue we dont have pk
CREATE TABLE TemperaturesByCity_Distinct LIKE TemperaturesByCity;
INSERT INTO TemperaturesByCity_Distinct
SELECT DISTINCT * 
FROM TemperaturesByCity;

-- Drop the original table and replace it with the distinct version
DROP TABLE TemperaturesByCity;

-- Check for null values in the distinct table
WITH cte_temp AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY dt, AverageTemperature, AverageTemperatureUncertainty, City, Country ORDER BY dt ASC) AS dup
    FROM TemperaturesByCity_Distinct
)
SELECT *
FROM cte_temp
WHERE AverageTemperature IS NULL;

-- Delete rows with null values in AverageTemperature
DELETE FROM TemperaturesByCity_Distinct
WHERE AverageTemperature IS NULL;

-- Check for remaining duplicates in the distinct table
WITH cte_temp AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY dt, AverageTemperature, AverageTemperatureUncertainty, City, Country ORDER BY dt ASC) AS dup
    FROM TemperaturesByCity_Distinct
)
SELECT *
FROM cte_temp
WHERE dup > 1;

-- Create a temporary table for Egypt with cleaned data
CREATE TEMPORARY TABLE egypt AS
SELECT *
FROM TemperaturesByCity_Distinct
WHERE Country = 'Egypt';

-- Calculate the average summer temperature for Cairo after 1900
SELECT YEAR(dt) AS year, 
       city,
       AVG(AverageTemperature) AS average_temp_if_summer
FROM egypt
WHERE city = 'Cairo' AND YEAR(dt) >= 1900 AND MONTH(dt) IN (6, 7, 8, 9)
GROUP BY YEAR(dt)
ORDER BY average_temp_if_summer DESC;

-- Calculate the average summer temperature for all cities in Egypt after 1900
SELECT YEAR(dt) AS year, 
       city,
       AVG(AverageTemperature) AS average_temp_of_summer
FROM egypt
WHERE YEAR(dt) >= 1900 AND MONTH(dt) IN (6, 7, 8, 9)
GROUP BY YEAR(dt), city
ORDER BY YEAR(dt) DESC;

-- Create a view to visualize summer temperatures in Egypt
CREATE VIEW temp_of_summer AS
SELECT YEAR(dt) AS year, 
       city,
       AVG(AverageTemperature) AS average_temp_of_summer
FROM TemperaturesByCity_Distinct
WHERE Country = 'Egypt'
  AND YEAR(dt) >= 1900
  AND MONTH(dt) IN (6, 7, 8, 9)
GROUP BY YEAR(dt), city
ORDER BY YEAR(dt) DESC;

-- Note: After visualizing the data in Power BI, it seems the dataset may be random or not real. 
-- The project provided valuable insights into data cleaning and importing processes.
