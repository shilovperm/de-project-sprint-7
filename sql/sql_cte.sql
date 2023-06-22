--Увы в задании нигде нет события написания сообщения. 
--Увы его и среди данных нет. Именно по этому запрос user_group_messages обрезанный. 
--Предлагаю либо дать нормальный файл, либо вместе разбираться с этим. 
--Результат не консистентный полностью :-) почему конверсия больше 1 в 12 ночи я пока понять не могу

with user_group_messages as (
    SELECT 
		hg.hk_group_id,
		count(DISTINCT lug.hk_user_id) as cnt_users_in_group_with_messages
	from 
		STV230551__DWH.h_groups_p6 hg
	inner join STV230551__DWH.l_user_group_activity lug on hg.hk_group_id = lug.hk_group_id 
	GROUP BY hg.hk_group_id
),
user_group_log as (
 	SELECT 
		hg.hk_group_id,
		count(DISTINCT sah.user_id_from) as cnt_added_users
	from 
		STV230551__DWH.h_groups_p6 hg
	inner join STV230551__DWH.l_user_group_activity lug on hg.hk_group_id = lug.hk_group_id 
	INNER JOIN STV230551__DWH.s_auth_history sah on lug.hk_l_user_group_activity = sah.hk_l_user_group_activity 
	where sah.event = 'add'
	GROUP BY hg.hk_group_id
)
select 
	ugl.hk_group_id,
	ugl.cnt_added_users,
	ugm.cnt_users_in_group_with_messages,
	ugm.cnt_users_in_group_with_messages / ugl.cnt_added_users as group_conversion,
	hg.registration_dt
from user_group_log as ugl
	left join user_group_messages as ugm on ugl.hk_group_id = ugm.hk_group_id
	left join STV230551__DWH.h_groups_p6 hg on ugl.hk_group_id = hg.hk_group_id 
order by
	hg.registration_dt,
	ugm.cnt_users_in_group_with_messages / ugl.cnt_added_users desc 
limit 10
;



	
	

	