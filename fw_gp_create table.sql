CREATE table "fw_kafka_buffer"
(
 subject varchar(100),
 org_type varchar(100),
 speed integer,
 obj_code varchar(100),
 date varchar(100),
 vol_down varchar(100),
 vol_up varchar(100),
)
WITH (
	appendonly=true,
	orientation=column,
	compresstype=zstd,
	compresslevel=3
)
distributed randomly;


drop table fw_kafka_buffer

select * from fw_kafka_buffer
limit 10

select count(*) as cnt, to_timestamp(date, 'yyyy-mm-dd') as date
from fw_kafka_buffer
group by 2
order by 2 asc

create materialized view mv_school_trafic_data as 
select subject,
		org_type,
		speed,
		obj_code,
		to_timestamp(date, 'yyyy-mm-dd') as date,
		cast(vol_down as numeric) as vol_down,
		cast(vol_up as numeric) as vol_up,
		cast(vol_down as numeric) + cast(vol_up as numeric) as trafic_volume
from fw_kafka_buffer

create materialized view mv_school_trafic_data_byRegion as 
select subject,
--		org_type,
--		speed,
--		obj_code,
		to_timestamp(date, 'yyyy-mm-dd') as date,
		sum(cast(vol_down as numeric)) as vol_down,
		sum(cast(vol_up as numeric)) as vol_up,
		sum(cast(vol_down as numeric) + cast(vol_up as numeric)) as trafic_volume
from fw_kafka_buffer
group by 1,2

create materialized view mv_school_trafic_data_byOrganization as 
select 
--		subject,
		org_type,
--		speed,
		count( distinct obj_code) as UniqObjCount,
		to_timestamp(date, 'yyyy-mm-dd') as date,
		sum(cast(vol_down as numeric)) as vol_down,
		sum(cast(vol_up as numeric)) as vol_up,
		sum(cast(vol_down as numeric) + cast(vol_up as numeric)) as trafic_volume
from fw_kafka_buffer
group by 1,3

create materialized view mv_school_trafic_uniqObjects as 
select 
--		subject,
		org_type,
--		speed,
		count( distinct obj_code) as UniqObjCount,
		to_timestamp(date, 'yyyy-mm-dd') as date
--		sum(cast(vol_down as numeric)) as vol_down,
--		sum(cast(vol_up as numeric)) as vol_up,
--		sum(cast(vol_down as numeric) + cast(vol_up as numeric)) as trafic_volume
from fw_kafka_buffer
group by 1,3




refresh materialized view mv_school_trafic_data

refresh materialized view mv_school_trafic_data_byRegion

refresh materialized view mv_school_trafic_data_byOrganization

refresh materialized view mv_school_trafic_uniqObjects



