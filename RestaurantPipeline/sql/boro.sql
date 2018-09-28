/*
INSERT into dim_boro (boro_name)
select distinct v_boro 
from restaurant_inbox 
order by v_boro
;
-- truncate table dim_boro 

*/

select * from dim_boro
-- drop table dim_boro
