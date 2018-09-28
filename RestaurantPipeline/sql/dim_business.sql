/*

-- drop TABLE dim_business;

-- truncate TABLE dim_business;

CREATE TABLE dim_business (
  business_id int(11) NOT NULL AUTO_INCREMENT,
  business_number int(11) NOT NULL,
  business_title varchar(150) DEFAULT NULL,
  address_number varchar(30) DEFAULT NULL,
  address_name varchar(100) DEFAULT NULL,
  zipcode varchar(5) DEFAULT NULL,
  -- phone varchar(15) DEFAULT NULL,  
  boro_id int(11) DEFAULT NULL,
  PRIMARY KEY (`business_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1


CREATE INDEX idx_dim_business_business_number
  ON dim_business (business_number);


*/

/*
select count(*) from (
select distinct 
v_business_number as business_number,
v_business_title as business_title,
v_address_number as address_number,
v_address_name as address_name,
v_zipcode as zipcode,
-- v_phone as phone,
b.boro_id
from restaurant_inbox a join dim_boro b on a.v_boro = b.boro_name
-- where b.boro_id is null

) v
*/


-- select * from dim_business


insert into dim_business (
business_number,
business_title,
address_number,
address_name,
zipcode,
boro_id
)
select distinct 
v_business_number as business_number,
v_business_title as business_title,
v_address_number as address_number,
v_address_name as address_name,
v_zipcode as zipcode,
b.boro_id
from restaurant_inbox a join dim_boro b on a.v_boro = b.boro_name
-- 27974
