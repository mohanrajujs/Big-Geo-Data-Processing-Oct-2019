
---- CREATION OF DATASET USED FOR EDA ----
 
set statement_timeout = '5000s'


--- Remove duplicates from the block data ---
Delete from block a
where a.ctid <> (select min (b.ctid)
				 from block as b
				 where a.geom = b.geom);


-- Creates observation table--
Create table s6040446.observation as 
select * from public.observation

Create table s6040446.days as 
select * from public.days

-- Merge block and observation table --
Insert into s6040446.Blck_Obs 
select * from 
(select * from observation as o
	join block as b ON b.block = o.block
) d

---- merge with block table ---
Create table s6040446.Newobservation as
(select * from observation as o
	join block as b ON b.block = o.blockk
)

---- merge with days table---
Create table s6040446.observed as
(select * from newobservation as o
	join days as d ON d.odate = o.obsdate)


-- data population for each date -- 
create table s6040446.observed_dates as(
Select o.obsdate, count(*) as date_count, o.geom, o.observer
From observed as o
Group by o.obsdate, o.geom, o.observer
order by o.obsdate asc)

--- merge observed dates with days ---
Create table s6040446.observed_days as
(select * from observed_dates as o
	join days as d ON d.odate = o.obsdate)


--- unique observers in the observation data---
Select o.observer, count(*) as obscount
From observation as o
Group by o.observer
Order by obscount


--- observation geometry data for each block ---
create table s6040446.observed_geom as
(select block.geom, count(observed.observer)as no_of_obs from observed
left join block on observed.block = block.block
group by block.geom)


--- observation for weekdays ---
create table s6040446.observed_weekdays as
(Select o.dow, count(*) as obscount
From observed as o
where o.dow = '1' or o.dow = '2' or o.dow = '3' or o.dow = '4' or o.dow = '5'
Group by o.dow
order by o.dow)

create table s6040446.obs_Wdays as(
select *
from observed as o
where o.dow = '1' or o.dow = '2' or o.dow = '3' or o.dow = '4' or o.dow = '5'
order by o.doy)

select count(*)
from obs_wdays

--- observation for weekends ---
create table s6040446.observed_weekends as
(Select o.dow, count(*) as obscount
From observed as o
where o.dow = '0' or o.dow = '6' 
Group by o.dow
order by o.dow)

create table s6040446.obs_Wends as(
select *
from observed as o
where o.dow = '0' or o.dow ='6'
order by o.doy)

select count(*)
from obs_wends

--- observation for national holidays ---
create table s6040446.observed_holidays as
(Select o.natholiday, count(*) as obscount
From observed as o
where o.natholiday = 'True' or o.natholiday = 'False' 
Group by o.natholiday
order by o.natholiday)

create table s6040446.obs_Hdys as(
select *
from observed as o
where o.natholiday = 'True' or o.natholiday = 'False' 
order by o.doy)


---- merge landuse data and observation data ---
Create table s6040446.block_landuse as 
select * from public.block_landuse
alter table block_landuse
rename block to blockk

create table s6040446.obs_landuse as(
select * from observed as o
	join block_landuse as b ON b.blockk = o.block)


---- merge temperature data and observation data ---
Create table s6040446.temperature as 
select * from public.temperature
alter table temperature
rename block to blockk
rename id to t_id

create table s6040446.obs_temp as(
select * from observed as o
	join temperature as t ON t.blockk = o.block)
	
	
---- merge precipitation data and observation data ---
Create table s6040446.precipitation as 
select * from public.precipitation
alter table precipitation
rename block to blockk
rename id to t_id

create table s6040446.obs_precip as(
select * from observed as o
	join precipitation as p ON t.blockk = o.block)	



--- count of observers ---
create table s6040446.observers as(
Select o.observer, count(*) as obscount
From observed as o
Group by o.observer
Order by obscount desc)
alter table observers 
rename observer to obs

create table s6040446.obs_count as (
select *
from observed as o, observers as b
where o.observer = b.obs)
alter table obs_count
rename obsdate to obdate


--- Aggregation: group per day ----
create table s6040446.obs_test as(
with test as (
	select obs_count.obdate
	from obs_count
	group by obs_count.obdate
	order by obs_count.obdate asc)
select *
from test as t, obs_count as o
where t.obdate = o.odate)




----Observation data for each day in the week----
Create table s6040446.observer_week as (with W1 as (Select o.observer as Id, count(*) as weekday
From observed as o
WHERE o.dow IN (1,2,3,4,5)
Group by o.observer),

W2 as (Select o.observer as Id, count(*) as weekend
From observed as o
WHERE o.dow IN (0,6)
Group by o.observer)

SELECT O.obs, weekday, weekend
FROM observers as O
LEFT JOIN W1
ON O.obs = W1.Id
LEFT JOIN W2
ON O.obs = W2.Id)



