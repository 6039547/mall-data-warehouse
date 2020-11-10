/*
Navicat MySQL Data Transfer

Source Server         : root@RuHuTian
Source Server Version : 50624
Source Host           : 192.168.163.144:3306
Source Database       : gmall

Target Server Type    : MYSQL
Target Server Version : 50624
File Encoding         : 65001

Date: 2020-11-04 21:33:44
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for ads_gmv_sum_day
-- ----------------------------
DROP TABLE IF EXISTS `ads_gmv_sum_day`;
CREATE TABLE `ads_gmv_sum_day` (
  `dt` varchar(200) DEFAULT NULL COMMENT '统计日期',
  `gmv_count` bigint(20) DEFAULT NULL COMMENT '当日gmv订单个数',
  `gmv_amount` decimal(16,2) DEFAULT NULL COMMENT '当日gmv订单总金额',
  `gmv_payment` decimal(16,2) DEFAULT NULL COMMENT '当日支付金额'
) ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='每日活跃用户数量';

-- ----------------------------
-- Table structure for ads_gmv_sum_province
-- ----------------------------
DROP TABLE IF EXISTS `ads_gmv_sum_province`;
CREATE TABLE `ads_gmv_sum_province` (
  `province` varchar(255) DEFAULT NULL,
  `gmv` bigint(255) DEFAULT NULL,
  `remark` varchar(255) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC;

-- ----------------------------
-- Table structure for ads_user_action_convert_day
-- ----------------------------
DROP TABLE IF EXISTS `ads_user_action_convert_day`;
CREATE TABLE `ads_user_action_convert_day` (
  `dt` varchar(200) DEFAULT NULL COMMENT '统计日期',
  `total_visitor_m_count` bigint(20) DEFAULT NULL COMMENT '总访问人数',
  `order_u_count` bigint(20) DEFAULT NULL COMMENT '下单人数',
  `visitor2order_convert_ratio` decimal(10,2) DEFAULT NULL COMMENT '购物车到下单转化率',
  `payment_u_count` bigint(20) DEFAULT NULL COMMENT '支付人数',
  `order2payment_convert_ratio` decimal(10,2) DEFAULT NULL COMMENT '下单到支付的转化率'
) ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='每日用户行为转化率统计';

-- ----------------------------
-- Table structure for ads_user_retention_day_rate
-- ----------------------------
DROP TABLE IF EXISTS `ads_user_retention_day_rate`;
CREATE TABLE `ads_user_retention_day_rate` (
  `stat_date` varchar(255) DEFAULT NULL COMMENT '统计日期',
  `create_date` varchar(255) DEFAULT NULL COMMENT '设备新增日期',
  `retention_day` bigint(200) DEFAULT NULL COMMENT '截止当前日期留存天数',
  `retention_count` bigint(200) DEFAULT NULL COMMENT '留存数量',
  `new_mid_count` bigint(200) DEFAULT NULL COMMENT '当日设备新增数量',
  `retention_ratio` decimal(10,2) DEFAULT NULL COMMENT '留存率'
) ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='每日用户留存情况';

-- ----------------------------
-- Table structure for ads_uv_count
-- ----------------------------
DROP TABLE IF EXISTS `ads_uv_count`;
CREATE TABLE `ads_uv_count` (
  `dt` varchar(255) DEFAULT NULL COMMENT '统计日期',
  `day_count` bigint(200) DEFAULT NULL COMMENT '当日用户数量',
  `wk_count` bigint(200) DEFAULT NULL COMMENT '当周用户数量',
  `mn_count` bigint(200) DEFAULT NULL COMMENT '当月用户数量',
  `is_weekend` varchar(200) DEFAULT NULL COMMENT 'Y,N是否是周末,用于得到本周最终结果',
  `is_monthend` varchar(200) DEFAULT NULL COMMENT 'Y,N是否是月末,用于得到本月最终结果'
) ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='每日活跃用户数量';

-- ----------------------------
-- Table structure for base_category1
-- ----------------------------
DROP TABLE IF EXISTS `base_category1`;
CREATE TABLE `base_category1` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '编号',
  `name` varchar(100) NOT NULL COMMENT '分类名称',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=19 DEFAULT CHARSET=utf8 COMMENT='一级分类表';

-- ----------------------------
-- Table structure for base_category2
-- ----------------------------
DROP TABLE IF EXISTS `base_category2`;
CREATE TABLE `base_category2` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '编号',
  `name` varchar(200) NOT NULL COMMENT '二级分类名称',
  `category1_id` bigint(20) DEFAULT NULL COMMENT '一级分类编号',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=126 DEFAULT CHARSET=utf8 COMMENT='二级分类表';

-- ----------------------------
-- Table structure for base_category3
-- ----------------------------
DROP TABLE IF EXISTS `base_category3`;
CREATE TABLE `base_category3` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '编号',
  `name` varchar(200) NOT NULL COMMENT '三级分类名称',
  `category2_id` bigint(20) DEFAULT NULL COMMENT '二级分类编号',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1150 DEFAULT CHARSET=utf8 COMMENT='三级分类表';

-- ----------------------------
-- Table structure for order_detail
-- ----------------------------
DROP TABLE IF EXISTS `order_detail`;
CREATE TABLE `order_detail` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '编号',
  `order_id` bigint(20) DEFAULT NULL COMMENT '订单编号',
  `sku_id` bigint(20) DEFAULT NULL COMMENT 'sku_id',
  `sku_name` varchar(200) DEFAULT NULL COMMENT 'sku名称（冗余)',
  `img_url` varchar(200) DEFAULT NULL COMMENT '图片名称（冗余)',
  `order_price` decimal(10,2) DEFAULT NULL COMMENT '购买价格(下单时sku价格）',
  `sku_num` varchar(200) DEFAULT NULL COMMENT '购买个数',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=utf8 COMMENT='订单明细表';

-- ----------------------------
-- Table structure for order_info
-- ----------------------------
DROP TABLE IF EXISTS `order_info`;
CREATE TABLE `order_info` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '编号',
  `consignee` varchar(100) DEFAULT NULL COMMENT '收货人',
  `consignee_tel` varchar(20) DEFAULT NULL COMMENT '收件人电话',
  `total_amount` decimal(10,2) DEFAULT NULL COMMENT '总金额',
  `order_status` varchar(20) DEFAULT NULL COMMENT '订单状态',
  `user_id` bigint(20) DEFAULT NULL COMMENT '用户id',
  `payment_way` varchar(20) DEFAULT NULL COMMENT '付款方式',
  `delivery_address` varchar(1000) DEFAULT NULL COMMENT '送货地址',
  `order_comment` varchar(200) DEFAULT NULL COMMENT '订单备注',
  `out_trade_no` varchar(50) DEFAULT NULL COMMENT '订单交易编号（第三方支付用)',
  `trade_body` varchar(200) DEFAULT NULL COMMENT '订单描述(第三方支付用)',
  `create_time` datetime DEFAULT NULL COMMENT '创建时间',
  `operate_time` datetime DEFAULT NULL COMMENT '操作时间',
  `expire_time` datetime DEFAULT NULL COMMENT '失效时间',
  `tracking_no` varchar(100) DEFAULT NULL COMMENT '物流单编号',
  `parent_order_id` bigint(20) DEFAULT NULL COMMENT '父订单编号',
  `img_url` varchar(200) DEFAULT NULL COMMENT '图片路径',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8 COMMENT='订单表 订单表';

-- ----------------------------
-- Table structure for payment_info
-- ----------------------------
DROP TABLE IF EXISTS `payment_info`;
CREATE TABLE `payment_info` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '编号',
  `out_trade_no` varchar(20) DEFAULT NULL COMMENT '对外业务编号',
  `order_id` varchar(20) DEFAULT NULL COMMENT '订单编号',
  `user_id` varchar(20) DEFAULT NULL COMMENT '用户编号',
  `alipay_trade_no` varchar(20) DEFAULT NULL COMMENT '支付宝交易流水编号',
  `total_amount` decimal(16,2) DEFAULT NULL COMMENT '支付金额',
  `subject` varchar(20) DEFAULT NULL COMMENT '交易内容',
  `payment_type` varchar(20) DEFAULT NULL COMMENT '支付方式',
  `payment_time` varchar(20) DEFAULT NULL COMMENT '支付时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=58651 DEFAULT CHARSET=utf8 COMMENT='支付流水表';

-- ----------------------------
-- Table structure for sku_info
-- ----------------------------
DROP TABLE IF EXISTS `sku_info`;
CREATE TABLE `sku_info` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '库存id(itemID)',
  `spu_id` bigint(20) DEFAULT NULL COMMENT '商品id',
  `price` decimal(10,0) DEFAULT NULL COMMENT '价格',
  `sku_name` varchar(200) DEFAULT NULL COMMENT 'sku名称',
  `sku_desc` varchar(2000) DEFAULT NULL COMMENT '商品规格描述',
  `weight` decimal(10,2) DEFAULT NULL COMMENT '重量',
  `tm_id` bigint(20) DEFAULT NULL COMMENT '品牌(冗余)',
  `category3_id` bigint(20) DEFAULT NULL COMMENT '三级分类id（冗余)',
  `sku_default_img` varchar(200) DEFAULT NULL COMMENT '默认显示图片(冗余)',
  `create_time` datetime DEFAULT NULL COMMENT '创建时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=11 DEFAULT CHARSET=utf8 COMMENT='库存单元表';

-- ----------------------------
-- Table structure for user_info
-- ----------------------------
DROP TABLE IF EXISTS `user_info`;
CREATE TABLE `user_info` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '编号',
  `login_name` varchar(200) DEFAULT NULL COMMENT '用户名称',
  `nick_name` varchar(200) DEFAULT NULL COMMENT '用户昵称',
  `passwd` varchar(200) DEFAULT NULL COMMENT '用户密码',
  `name` varchar(200) DEFAULT NULL COMMENT '用户姓名',
  `phone_num` varchar(200) DEFAULT NULL COMMENT '手机号',
  `email` varchar(200) DEFAULT NULL COMMENT '邮箱',
  `head_img` varchar(200) DEFAULT NULL COMMENT '头像',
  `user_level` varchar(200) DEFAULT NULL COMMENT '用户级别',
  `birthday` date DEFAULT NULL COMMENT '用户生日',
  `gender` varchar(1) DEFAULT NULL COMMENT '性别 M男,F女',
  `create_time` datetime DEFAULT NULL COMMENT '创建时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=utf8 COMMENT='用户表';

-- ----------------------------
-- Procedure structure for init_data
-- ----------------------------
DROP PROCEDURE IF EXISTS `init_data`;
DELIMITER ;;
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
 END
;;
DELIMITER ;

-- ----------------------------
-- Procedure structure for insert_order
-- ----------------------------
DROP PROCEDURE IF EXISTS `insert_order`;
DELIMITER ;;
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
 END
;;
DELIMITER ;

-- ----------------------------
-- Procedure structure for insert_payment
-- ----------------------------
DROP PROCEDURE IF EXISTS `insert_payment`;
DELIMITER ;;
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
 END
;;
DELIMITER ;

-- ----------------------------
-- Procedure structure for insert_sku
-- ----------------------------
DROP PROCEDURE IF EXISTS `insert_sku`;
DELIMITER ;;
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
 END
;;
DELIMITER ;

-- ----------------------------
-- Procedure structure for insert_user
-- ----------------------------
DROP PROCEDURE IF EXISTS `insert_user`;
DELIMITER ;;
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
 END
;;
DELIMITER ;

-- ----------------------------
-- Procedure structure for update_order
-- ----------------------------
DROP PROCEDURE IF EXISTS `update_order`;
DELIMITER ;;
CREATE DEFINER=`root`@`localhost` PROCEDURE `update_order`(operate_time_string VARCHAR(20))
BEGIN 
  DECLARE v_operate_time DATETIME DEFAULT NULL; 
 SET v_operate_time=DATE_FORMAT(operate_time_string,'%Y-%m-%d');
UPDATE order_info o SET   o.`order_status`=o.`order_status`+rand_num_seed(0,1,o.id) ,operate_time= IF( rand_num_seed(0,1,o.id) >0 , DATE_ADD(v_operate_time ,INTERVAL rand_num(30,20*3600) SECOND),operate_time)
WHERE o.`order_status`<5;
END
;;
DELIMITER ;

-- ----------------------------
-- Function structure for rand_num
-- ----------------------------
DROP FUNCTION IF EXISTS `rand_num`;
DELIMITER ;;
CREATE DEFINER=`root`@`localhost` FUNCTION `rand_num`(from_num INT ,to_num INT) RETURNS int(11)
BEGIN   
 DECLARE i INT DEFAULT 0;  
 SET i = FLOOR(from_num +RAND()*(to_num -from_num+1))   ;
RETURN i;  
 END
;;
DELIMITER ;

-- ----------------------------
-- Function structure for rand_num_seed
-- ----------------------------
DROP FUNCTION IF EXISTS `rand_num_seed`;
DELIMITER ;;
CREATE DEFINER=`root`@`localhost` FUNCTION `rand_num_seed`(from_num INT ,to_num INT,seed LONG ) RETURNS int(11)
BEGIN   
 DECLARE i INT DEFAULT 0;  
 SET i = FLOOR(from_num +RAND(seed+UNIX_TIMESTAMP(NOW()))*(to_num -from_num+1))   ;
RETURN i;  
 END
;;
DELIMITER ;

-- ----------------------------
-- Function structure for rand_nums
-- ----------------------------
DROP FUNCTION IF EXISTS `rand_nums`;
DELIMITER ;;
CREATE DEFINER=`root`@`localhost` FUNCTION `rand_nums`(from_num INT ,to_num INT,n INT ,delemit VARCHAR(20)) RETURNS varchar(255) CHARSET utf8
BEGIN   
 DECLARE i INT DEFAULT 0;  
 DECLARE v INT DEFAULT 0;
 DECLARE return_str VARCHAR(255) DEFAULT '';  
 WHILE i < n DO 
	 SET v = rand_num (from_num   ,to_num  ) ;
	 SET return_str=CONCAT(return_str,v);
	 IF LENGTH(return_str)>0 THEN 
	   SET return_str=CONCAT(return_str,delemit) ;
	 END IF;
	 SET i = i + 1;
END WHILE;
 RETURN return_str;
END
;;
DELIMITER ;

-- ----------------------------
-- Function structure for rand_string
-- ----------------------------
DROP FUNCTION IF EXISTS `rand_string`;
DELIMITER ;;
CREATE DEFINER=`root`@`localhost` FUNCTION `rand_string`(n INT) RETURNS varchar(255) CHARSET utf8
BEGIN    
DECLARE chars_str VARCHAR(100) DEFAULT 'abcdefghijklmnopqrstuvwxyzABCDEFJHIJKLMNOPQRSTUVWXYZ';
 DECLARE return_str VARCHAR(255) DEFAULT '';
 DECLARE i INT DEFAULT 0;
 WHILE i < n DO  
 SET return_str =CONCAT(return_str,SUBSTRING(chars_str,FLOOR(1+RAND()*52),1));  
 SET i = i + 1;
 END WHILE;
 RETURN return_str;
END
;;
DELIMITER ;
