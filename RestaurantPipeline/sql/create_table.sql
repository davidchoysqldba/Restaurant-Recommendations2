-- select * from restaurant_inbox.dim_boro
 
-- select * from restaurant_inbox.dim_business
 
 select business_number, business_title, address_number, address_name, zipcode, boro_id, record_date from trans_dim_business
 
 select * from restaurant_inbox.trans_dim_business
 
-- select * from trans_dim_business a join dim_boro b on a.boro = b.boro_name
 
 
 /*
 
 CREATE TABLE restaurant_inbox.`dim_boro` (
  `boro_id` int(11) NOT NULL AUTO_INCREMENT,
  `boro_name` varchar(20) DEFAULT NULL,
  PRIMARY KEY (`boro_id`)
) ENGINE=InnoDB AUTO_INCREMENT=8 DEFAULT CHARSET=latin1

 */
 
 /*
 
 CREATE TABLE restaurant_inbox.`trans_dim_business` (
  `trans_id` int(11) NOT NULL AUTO_INCREMENT,
  `business_number` int(11) NOT NULL,
  `business_title` varchar(150) DEFAULT NULL,
  `address_number` varchar(30) DEFAULT NULL,
  `address_name` varchar(100) DEFAULT NULL,
  `zipcode` varchar(5) DEFAULT NULL,
  `boro` varchar(20) DEFAULT NULL,
  `boro_id` int(11) DEFAULT NULL,
  `record_date` datetime DEFAULT NULL,
  PRIMARY KEY (`trans_id`)
) ENGINE=InnoDB AUTO_INCREMENT=9655 DEFAULT CHARSET=utf8

 */
 
 /*
 CREATE TABLE restaurant_inbox.`dim_business` (
  `business_id` int(11) NOT NULL AUTO_INCREMENT,
  `business_number` int(11) NOT NULL,
  `business_title` varchar(150) DEFAULT NULL,
  `address_number` varchar(30) DEFAULT NULL,
  `address_name` varchar(100) DEFAULT NULL,
  `zipcode` varchar(5) DEFAULT NULL,
  `boro_id` int(11) DEFAULT NULL,
  record_date datetime DEFAULT NULL,
  PRIMARY KEY (`business_id`),
  KEY `idx_dim_business_business_number` (`business_number`)
) ENGINE=InnoDB AUTO_INCREMENT=27975 DEFAULT CHARSET=latin1
*/