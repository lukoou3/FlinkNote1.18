-- 建表

create table tsg_galaxy_v3.test_uniq(
    datetime DateTime,
    dinctinct_count AggregateFunction(uniqCombined64State(12), String)
)
ENGINE=MergeTree
PARTITION BY toYYYYMMDD(datetime)
ORDER BY (datetime)
;


create table tsg_galaxy_v3.test_uniq_detail(
    datetime DateTime,
    user_id String
)
ENGINE=MergeTree
PARTITION BY toYYYYMMDD(datetime)
ORDER BY (datetime)
;

-- 插入明细数据

insert into tsg_galaxy_v3.test_uniq_detail(datetime, user_id) values
('2024-01-25 13:00:00', '1'), 
('2024-01-25 13:00:00', '2'), 
('2024-01-25 13:00:00', '3'), 
('2024-01-25 14:00:00', '1'), 
('2024-01-25 14:00:00', '3'),
('2024-01-25 14:00:00', '4'),
('2024-01-26 13:00:00', 'a'), 
('2024-01-26 13:00:00', 'b'), 
('2024-01-26 13:00:00', 'c'), 
('2024-01-26 14:00:00', 'a'), 
('2024-01-26 14:00:00', 'c'),
('2024-01-26 14:00:00', 'd'),
;

SELECT * from  tsg_galaxy_v3.test_uniq_detail;

-- 预聚合dinctinct_count存储hll

INSERT INTO tsg_galaxy_v3.test_uniq 
SELECT 
     date_trunc('hour', datetime) day,
     uniqCombined64State(12)(user_id) dinctinct_count
FROM tsg_galaxy_v3.test_uniq_detail 
where datetime >= '2024-01-25 00:00:00' and datetime < '2024-01-27 00:00:00'
GROUP BY date_trunc('hour', datetime)
;


---------------查询测试

SELECT 
     date_trunc('hour', datetime) datetime,
     uniqCombined64(user_id) count
FROM tsg_galaxy_v3.test_uniq_detail 
where datetime >= '2024-01-25 00:00:00' and datetime < '2024-01-27 00:00:00'
GROUP BY 1

datetime               |count|
-----------------------+-----+
2024-01-26 13:00:00.000|    3|
2024-01-25 13:00:00.000|    3|
2024-01-25 14:00:00.000|    3|
2024-01-26 14:00:00.000|    3|

SELECT 
    datetime, 
    uniqCombined64Merge(12)(dinctinct_count) count
from tsg_galaxy_v3.test_uniq
GROUP BY datetime
;

datetime               |count|
-----------------------+-----+
2024-01-26 13:00:00.000|    3|
2024-01-25 13:00:00.000|    3|
2024-01-25 14:00:00.000|    3|
2024-01-26 14:00:00.000|    3|

SELECT 
     date_trunc('day', datetime) datetime,
     uniqCombined64(user_id) count
FROM tsg_galaxy_v3.test_uniq_detail 
where datetime >= '2024-01-25 00:00:00' and datetime < '2024-01-27 00:00:00'
GROUP BY 1
;

datetime               |count|
-----------------------+-----+
2024-01-25 00:00:00.000|    4|
2024-01-26 00:00:00.000|    4|

SELECT 
    date_trunc('day', datetime) datetime,
    uniqCombined64Merge(12)(dinctinct_count) count
from tsg_galaxy_v3.test_uniq
GROUP BY 1
;

datetime               |count|
-----------------------+-----+
2024-01-25 00:00:00.000|    4|
2024-01-26 00:00:00.000|    4|

