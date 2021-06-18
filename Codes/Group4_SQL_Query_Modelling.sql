---- CRETAION OF THE RF TABLE FOR INPUT IN MODELLING -----

--- temperature and precipitation ---
with tempe as (select t.blockk, avg(temper)
from temperature as t
group by t.blockk)

with precip as (select p.block, avg(precip)
from precipitation as p
group by p.block)


--- merge temperature and precipitation data ---
create table s6040446.temprecip as (select t.blockk as Block, avg(temper) as temper,avg(precip) as precip
from temperature as t, precipitation as p
where t.blockk = p.block
group by t.blockk)

--- block_road_access duplicate removed ---
create table s6040446.road as (select r1.block as block, sum(roadlength) as roadlength
from block_road_access as r1
group by r1.block)


--- road ---
create table s6040446.dataset as(with roadd as (select t.*, r1.roadlength
from temprecip as t, road as r1
where t.Block = r1.block,

Obs as (select o1.blockk as Block, count(*) as obscount
from observation as o1
group by o1.blockk)

select o.block as block, o.obscount as obscount, r.temper as temp, r.precip as precip, r.roadlength as road_length
from Obs as o
left join roadd as r
on r.block = o.Block)



--- landuse ---
create table s6040446.RF as (with landuse as (select blockk, max(category) as category
from block_landuse
group by blockk
)

--- merged dataset for RF ---
select *
from dataset as d
left join landuse as l
on l.blockk = d.block)



---- unique observer per block --- 
create table s6040446.Block_obs as(
select o.blockk, count (distinct (o.observer))
from observation as o
group by o.blockk)