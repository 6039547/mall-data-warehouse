DELIMITER $$
 
DROP FUNCTION IF EXISTS `rand_num`$$
##得到 from_num到to_num的整数 ，前包后包
CREATE DEFINER=`root`@`localhost` FUNCTION `rand_num`(from_num INT ,to_num INT) RETURNS INT(11)
BEGIN   
 DECLARE i INT DEFAULT 0;  
 SET i = FLOOR(from_num +RAND()*(to_num -from_num+1))   ;
RETURN i;  
 END$$

DELIMITER ;

DELIMITER $$

DROP FUNCTION IF EXISTS `rand_num_seed`$$

CREATE DEFINER=`root`@`localhost` FUNCTION `rand_num_seed`(from_num INT ,to_num INT,seed LONG ) RETURNS INT(11)
BEGIN   
 DECLARE i INT DEFAULT 0;  
 SET i = FLOOR(from_num +RAND(seed+UNIX_TIMESTAMP(NOW()))*(to_num -from_num+1))   ;
RETURN i;  
 END$$

DELIMITER ;


DELIMITER $$


DROP FUNCTION IF EXISTS `rand_nums`$$
##随机产生多个数字用分隔符分开 
CREATE DEFINER=`root`@`localhost` FUNCTION `rand_nums`(from_num INT ,to_num INT,n INT ,delemit VARCHAR(20)) RETURNS VARCHAR(255) CHARSET utf8
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
END$$

DELIMITER ;


DELIMITER $$

DROP FUNCTION IF EXISTS `rand_string`$$

CREATE DEFINER=`root`@`localhost` FUNCTION `rand_string`(n INT) RETURNS VARCHAR(255) CHARSET utf8
BEGIN    
DECLARE chars_str VARCHAR(100) DEFAULT 'abcdefghijklmnopqrstuvwxyzABCDEFJHIJKLMNOPQRSTUVWXYZ';
 DECLARE return_str VARCHAR(255) DEFAULT '';
 DECLARE i INT DEFAULT 0;
 WHILE i < n DO  
 SET return_str =CONCAT(return_str,SUBSTRING(chars_str,FLOOR(1+RAND()*52),1));  
 SET i = i + 1;
 END WHILE;
 RETURN return_str;
END$$

DELIMITER ;