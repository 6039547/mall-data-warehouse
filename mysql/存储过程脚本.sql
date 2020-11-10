

DELIMITER $$


DROP PROCEDURE IF EXISTS `insert_sku`$$
##新增max_num个sku
CREATE DEFINER=`root`@`localhost` PROCEDURE `insert_sku`( create_time_string VARCHAR(200),max_num INT  )
BEGIN  
 DECLARE v_create_time DATETIME DEFAULT NULL;
 DECLARE i INT DEFAULT 0;
 SET autocommit = 0;    
 REPEAT  
 SET i = i + 1;  
 SET v_create_time=DATE_ADD(DATE_FORMAT(create_time_string,'%Y-%m-%d') ,INTERVAL rand_num(1,3600*24) SECOND); 
 INSERT INTO sku_info (spu_id,price,sku_name,sku_desc,weight,tm_id,category3_id,sku_default_img,create_time  ) 
 VALUES (rand_num(1,1000) ,rand_num(10,5000) , rand_string(20), rand_string(30),CAST(rand_num(50,500) AS DECIMAL(10,2))/100.0  ,rand_num(1,100),  rand_num(1,5000),CONCAT('http://',rand_string(40)), v_create_time    ); 
 UNTIL i = max_num  
 END REPEAT;  
 COMMIT;  
 END$$

DELIMITER ;




DELIMITER $$

 

DROP PROCEDURE IF EXISTS `insert_user`$$
#随机产生max_num个用户
CREATE DEFINER=`root`@`localhost` PROCEDURE `insert_user`( create_time_string VARCHAR(200),max_num INT  )
BEGIN  
 DECLARE v_create_time DATETIME DEFAULT NULL;
 DECLARE i INT DEFAULT 0;
 DECLARE v_birthday DATE DEFAULT 0;
 DECLARE v_gender VARCHAR(1) DEFAULT NULL;
 SET autocommit = 0;    
 REPEAT  
   SET i  = i + 1;  
   SET v_create_time=DATE_ADD(DATE_FORMAT(create_time_string,'%Y-%m-%d') ,INTERVAL rand_num(1,3600*24) SECOND); 
   SET v_birthday=DATE_ADD(DATE_FORMAT('1950-01-01','%Y-%m-%d') ,INTERVAL rand_num(1,365*50) DAY); 
   SET v_gender=IF(rand_num(0,1)=0,'M','F');
 INSERT INTO user_info (login_name,nick_name,passwd,NAME,phone_num,email,head_img,user_level,birthday,gender,create_time  ) 
 VALUES (rand_string(20) ,rand_string(20) , CONCAT('pwd',rand_string(20)), rand_string(30), CONCAT('13',rand_nums(0,9,9,''))    ,CONCAT(rand_string(8),'@',rand_string(3),'.com') ,  CONCAT('http://',rand_string(40)), rand_num(1,5),v_birthday,v_gender,v_create_time    ); 
 UNTIL i = max_num  
 END REPEAT;  
 COMMIT;  
 END$$

DELIMITER ;



DELIMITER $$

  

DROP PROCEDURE IF EXISTS `insert_order`$$

##生成订单 
CREATE DEFINER=`root`@`localhost` PROCEDURE `insert_order`( create_time_string VARCHAR(200),max_num INT,user_num INT ,sku_num INT  )
BEGIN  
 DECLARE v_create_time DATETIME DEFAULT NULL;
  DECLARE i INT DEFAULT 0;
 DECLARE v_order_status INT DEFAULT 0;  
  DECLARE v_operate_time DATETIME DEFAULT NULL; 
    DECLARE v_order_id INT DEFAULT NULL; 
     DECLARE v_order_detail_num INT DEFAULT NULL; 
      DECLARE j INT DEFAULT 0;
 SET autocommit = 0;    
 REPEAT  
 SET i = i + 1;  
 SET v_create_time=DATE_ADD(DATE_FORMAT(create_time_string,'%Y-%m-%d') ,INTERVAL rand_num(30,3600*23) SECOND);
 SET v_order_status=rand_num(1,2); ## 
  IF v_order_status>1 THEN 
     SET v_operate_time= DATE_ADD(v_create_time ,INTERVAL rand_num(30,3600) SECOND);
   ELSE 
     SET v_operate_time=NULL  ;
   END IF ;
 INSERT INTO order_info (consignee, consignee_tel,total_amount ,order_status ,user_id,payment_way,delivery_address,order_comment,out_trade_no,trade_body,create_time,operate_time,expire_time, tracking_no,parent_order_id ,img_url) 
 VALUES (rand_string(6) , CONCAT('13',rand_nums(0,9,9,'')),CAST(rand_num(50,1000) AS DECIMAL(10,2)) ,v_order_status ,rand_num(1,user_num), rand_num(1,2),rand_string(20),rand_string(20),rand_nums(0,9,10,''),'',v_create_time, v_operate_time,NULL,NULL,NULL,NULL ); 
  SELECT  LAST_INSERT_ID() INTO v_order_id ;
     SET v_order_detail_num=rand_num(1,5); 
    WHILE j<v_order_detail_num DO
       SET j=j+1;
    INSERT INTO  order_detail  (order_id , sku_id,sku_name ,img_url ,order_price,sku_num ) 
    VALUES (v_order_id , rand_num(1,sku_num),rand_string(10),CONCAT('http://',rand_string(40)) ,CAST(rand_num(20,5000) AS DECIMAL(10,2)), rand_num(1,5)  ); 
    END WHILE;
    SET j=0;
 UNTIL i = max_num  
 END REPEAT;  
 COMMIT;  
 END$$

DELIMITER ;


DELIMITER $$
 
DROP PROCEDURE IF EXISTS `update_order`$$
## 随机让订单状态小于5的订单 发生状态改变
CREATE DEFINER=`root`@`localhost` PROCEDURE `update_order`(operate_time_string VARCHAR(20))
BEGIN 
  DECLARE v_operate_time DATETIME DEFAULT NULL; 
 SET v_operate_time=DATE_FORMAT(operate_time_string,'%Y-%m-%d');
UPDATE order_info o SET   o.`order_status`=o.`order_status`+rand_num_seed(0,1,o.id) ,operate_time= IF( rand_num_seed(0,1,o.id) >0 , DATE_ADD(v_operate_time ,INTERVAL rand_num(30,20*3600) SECOND),operate_time)
WHERE o.`order_status`<5;
END$$

DELIMITER ;


DELIMITER$$
 DROP PROCEDURE IF EXISTS `insert_payment`$$
 ## 只要订单状态更新为2 ，给当天插入一条支付信息 
 CREATE DEFINER=`root`@`localhost` PROCEDURE `insert_payment`( do_date_str VARCHAR(200)   )
BEGIN      
 INSERT INTO payment_info (out_trade_no,order_id,user_id,alipay_trade_no,total_amount,`subject`,payment_type,payment_time  ) 
  SELECT o.out_trade_no,o.id,user_id,
   CONCAT( rand_string(4),'-',rand_nums(0,9,8,'')) alipay_trade_no,
   o.total_amount,
   rand_string(8) `subject`,
  ( CASE rand_num(1,3) WHEN 1 THEN  'wechatpay' WHEN 2 THEN 'alipay' WHEN 3 THEN 'unionpay' END) payment_type , 
  IF(o.operate_time IS NULL,o.create_time,o.operate_time) payment_time
  FROM order_info  o 
  WHERE (DATE_FORMAT(o.create_time,'%Y-%m-%d')= do_date_str OR DATE_FORMAT(o.operate_time,'%Y-%m-%d')= do_date_str ) AND o.order_status='2';
  COMMIT;
 END$$

DELIMITER ;


DELIMITER $$
 
DROP PROCEDURE IF EXISTS `init_data`$$

CREATE DEFINER=`root`@`localhost` PROCEDURE `init_data`( do_date_string VARCHAR(20) ,order_incr_num INT,user_incr_num INT ,sku_num INT ,if_truncate BOOLEAN  )
BEGIN  
     DECLARE user_count INT DEFAULT 0; 
     DECLARE sku_count INT DEFAULT 0; 
     DECLARE do_date VARCHAR(20) DEFAULT do_date_string;
     IF if_truncate  THEN 
        TRUNCATE TABLE order_info ;
        TRUNCATE TABLE order_detail ;
        TRUNCATE TABLE sku_info ;
        TRUNCATE TABLE user_info ;
     END IF ;     
     CALL insert_sku(do_date,sku_num );
     SELECT COUNT(*) INTO sku_count FROM  sku_info;
     CALL insert_user(do_date,user_incr_num );
     SELECT COUNT(*) INTO user_count FROM  user_info;
     CALL update_order(do_date);
     CALL insert_order(do_date,order_incr_num,user_count,sku_count);
     CALL insert_payment(do_date);
 END$$

DELIMITER ;





