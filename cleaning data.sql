CREATE TABLE TemperaturesByCity (
    dt DATE NOT NULL,
    AverageTemperature double NULL,
    AverageTemperatureUncertainty double NULL,
    City VARCHAR(255) NOT NULL,
    Country VARCHAR(255) NOT NULL,
    Latitude VARCHAR(255) NULL,
    Longitude VARCHAR(255) NULL
);


CREATE TABLE TemperaturesByCountry (
    dt DATE NOT NULL,
    AverageTemperature double NULL,
    AverageTemperatureUncertainty double NULL,
    Country VARCHAR(255) NOT NULL
);

CREATE TABLE TemperaturesByMajorCity (
    dt DATE NOT NULL,
    AverageTemperature double NULL,
    AverageTemperatureUncertainty double NULL,
    City VARCHAR(255) NOT NULL,
    Country VARCHAR(255) NOT NULL,
    Latitude VARCHAR(255) NULL,
    Longitude VARCHAR(255) NULL
);


CREATE TABLE TemperaturesByState (
    dt DATE NOT NULL,
    AverageTemperature double NULL,
    AverageTemperatureUncertainty double NULL,
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




LOAD DATA INFILE 'E:\\climate change\\GlobalLandTemperaturesByCity.csv'
INTO TABLE temperaturesbycountry
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;


-- exploring the data
select country
from temperaturesbycountry
where dt='1824-01-01' and AverageTemperature=25.575;

-- disble safe mode
set SQL_SAFE_UPDATES=0;


-- updated corupeted cell from importing the data
update temperaturesbycountry
set temperaturesbycountry.Country='"Bonaire, Saint Eustatius And Saba"'
where dt='1824-01-01' and AverageTemperature=25.575;



--  exploring the data
select distinct country
from temperaturesbycountry;

-- not so helpfull i wanted to fined egypt lets look for cairo
select *
from temperaturesbycity;

-- there is country coulm here lets try to fing egypt
select *
from temperaturesbycity
where Country like "%gypt%";



-- lets create temp table to use it for egypt so the quere is faster
-- create view egypt as 
CREATE TEMPORARY TABLE egypt AS
select  *
from temperaturesbycity
where Country ="Egypt";


-- now lets see how many city we have
select distinct  city
from egypt;

-- the data not for all egypt but lets see it
select *
from egypt
where city='cairo' and year(dt)=1900
order by dt desc;

-- lets see dublicate
with cte_temp as(
select *,  ROW_NUMBER() over(partition by dt,AverageTemperature,AverageTemperatureUncertainty,City,Country order by dt asc) as dup
from temperaturesbycity
)
select *
from cte_temp;

-- beause we dont have pk we wont be able to update it so we will insert into new table insted
create table temperaturesbycity_distinct like temperaturesbycity;
insert into temperaturesbycity_distinct
select distinct * 
from temperaturesbycity;

-- lets drop temperaturesbycity and replace it with temperaturesbycity_distinct
drop table temperaturesbycity;

-- lets see null values
with cte_temp as(
select *,  ROW_NUMBER() over(partition by dt,AverageTemperature,AverageTemperatureUncertainty,City,Country order by dt asc) as dup
from temperaturesbycity_distinct
)
select *
from cte_temp
where AverageTemperature is null;


-- delete null valuse 
delete from temperaturesbycity_distinct
where AverageTemperature is null;

-- lets see if there still dublicate
with cte_temp as(
select *,  ROW_NUMBER() over(partition by dt,AverageTemperature,AverageTemperatureUncertainty,City,Country order by dt asc) as dup
from temperaturesbycity_distinct
)
select *
from cte_temp
where dup>1;

-- lets create temp table to use it for egypt so the quere is faster
-- create view egypt as 
CREATE TEMPORARY TABLE egypt AS
select  *
from temperaturesbycity_distinct
where Country ="Egypt";

-- lets see ave temp of summer after year 1900 for cairo
select year(dt) as year, city,avg(AverageTemperature) as average_temp_if_summer
from egypt
where city='cairo' and year(dt)>=1900 and month(dt) in (6,7,8,9)
group by year(dt)
order by average_temp_if_summer desc;

-- lets see ave temp of summer after year 1900 for all of egypt
select year(dt) as year, city,avg(AverageTemperature) as average_temp_of_summer
from egypt
where year(dt)>=1900 and month(dt) in (6,7,8,9)
group by year(dt),city
order by year desc;


-- lets creat a view to see its visualization on power bi
create view temp_of_summer as (
select year(sub.dt) as year, sub.city,avg(sub.AverageTemperature) as average_temp_of_summer
from (
		select  *
		from temperaturesbycity_distinct
		where Country ="Egypt"
		) sub
where year(sub.dt)>=1900 and month(sub.dt) in (6,7,8,9)
group by year(sub.dt),sub.city
order by year desc
);


-- after seeing the visulization it seems that this data is randow and its not real i found it on kagel but i learned from this project alot about data cleaing and
-- and importing data to mysql 


