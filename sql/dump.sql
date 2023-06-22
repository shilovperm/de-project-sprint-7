-- STV230551__STAGING.groups definition

CREATE TABLE STV230551__STAGING.group_log
(
    group_id  int NOT NULL,
    user_id  int NOT NULL,
    user_id_from  int NOT NULL,
    event varchar(10) not null,
    "datetime"  timestamp NOT NULL,
    CONSTRAINT C_PRIMARY PRIMARY KEY (group_id) DISABLED
)
order by
	group_id, 
	user_id,
	user_id_from
segmented by 
        hash(group_id) all nodes
PARTITION BY "datetime"::date
GROUP BY calendar_hierarchy_day("datetime"::date, 3, 2);

select * from STV230551__STAGING.group_log;

---------------------h_users_p6

drop table if exists STV230551__DWH.h_users_p6;

create table STV230551__DWH.h_users_p6
(
    hk_user_id bigint primary key,
    user_id      int,
    registration_dt datetime,
    load_dt datetime,
    load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_user_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);
;

INSERT INTO STV230551__DWH.h_users_p6(hk_user_id, user_id,registration_dt,load_dt,load_src)
select
       hash(user_id) as  hk_user_id,
       user_id as user_id,
       min("datetime") as registration_dt,
       now() as load_dt,
       's3' as load_src
       from STV230551__STAGING.group_log
where hash(user_id) not in (select hk_user_id from STV230551__DWH.h_users_p6)
group by 1,2,4,5; 

---------------------h_groups_p6

drop table if exists STV230551__DWH.h_groups_p6;

create table STV230551__DWH.h_groups_p6
(
    hk_group_id bigint primary key,
    group_id      int,
    registration_dt datetime,
    load_dt datetime,
    load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_group_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);
;

INSERT INTO STV230551__DWH.h_groups_p6(hk_group_id, group_id, registration_dt, load_dt, load_src)
select
       hash(group_id) as  hk_group_id,
       group_id as group_id,
       min("datetime") as registration_dt,
       now() as load_dt,
       's3' as load_src
       from STV230551__STAGING.group_log
where hash(group_id) not in (select hk_group_id from STV230551__DWH.h_groups_p6)
group by 1,2,4,5; 

----------------------l_user_group_activity

drop table if exists STV230551__DWH.l_user_group_activity;

create table STV230551__DWH.l_user_group_activity
(
	hk_l_user_group_activity bigint primary key,
	hk_user_id  bigint not null CONSTRAINT fk_l_user_group_activity_user REFERENCES STV230551__DWH.h_users_p6 (hk_user_id),
	hk_group_id bigint not null CONSTRAINT fk_l_user_group_activity_group REFERENCES STV230551__DWH.h_groups_p6 (hk_group_id),
	load_dt datetime,
	load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_user_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2); 

truncate table STV230551__DWH.l_user_group_activity;

INSERT INTO STV230551__DWH.l_user_group_activity(hk_l_user_group_activity, hk_user_id, hk_group_id, load_dt, load_src)
select
	distinct hash(hu.hk_user_id,	hg.hk_group_id) as hk_l_user_group_activity,
	hu.hk_user_id,
	hg.hk_group_id,	
	now() as load_dt,
	's3' as load_src
from STV230551__STAGING.group_log as gl
left join STV230551__DWH.h_users_p6 as hu on gl.user_id = hu.user_id
left join STV230551__DWH.h_groups_p6 as hg on gl.group_id = hg.group_id
where hash(hu.hk_user_id, hg.hk_group_id) not in (select hk_l_user_group_activity from STV230551__DWH.l_user_group_activity);

-----------------------------s_auth_history
drop table if exists STV230551__DWH.s_auth_history;

create table STV230551__DWH.s_auth_history
(
	hk_l_user_group_activity bigint not null CONSTRAINT fk_s_auth_history_l_user_group_activity REFERENCES STV230551__DWH.l_user_group_activity (hk_l_user_group_activity),
	user_id_from int NOT NULL,
	event varchar(10) not null,
    event_dt timestamp NOT NULL,
	load_dt datetime,
	load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_l_user_group_activity all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

truncate table STV230551__DWH.s_auth_history;

INSERT INTO STV230551__DWH.s_auth_history(hk_l_user_group_activity, user_id_from, event, event_dt, load_dt,load_src)
select 
	luga.hk_l_user_group_activity as hk_l_user_group_activity,
	gl.user_id_from ,
	gl.event,
	gl.datetime as event_dt,
	now() as load_dt,
	's3' as load_src
from STV230551__STAGING.group_log as gl
left join STV230551__DWH.h_groups_p6 as hg on gl.group_id = hg.group_id
left join STV230551__DWH.h_users_p6 as hu on gl.user_id = hu.user_id
left join STV230551__DWH.l_user_group_activity as luga on hg.hk_group_id = luga.hk_group_id and hu.hk_user_id = luga.hk_user_id
where luga.hk_l_user_group_activity not in (select hk_l_user_group_activity from STV230551__DWH.s_auth_history) ; 

select count(1) from STV230551__DWH.s_auth_history; 
--155755
--155755