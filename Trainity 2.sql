create database job_analysis;
use job_analysis;
CREATE TABLE job_data (
    ds VARCHAR(20),
    job_id INT,
    actor_id INT,
    event VARCHAR(50),
    language VARCHAR(50),
    time_spent INT,
    org VARCHAR(10)
);

select * from job_data;
SELECT 
    DATE_FORMAT(STR_TO_DATE(ds, '%m/%d/%Y'), '%Y-%m-%d') AS review_date,
    SECOND(ds) AS review_hour,
    COUNT(job_id) AS jobs_reviewed
FROM 
    job_data
WHERE 
    DATE_FORMAT(STR_TO_DATE(ds, '%m/%d/%Y'), '%Y-%m') = '2020-11'
GROUP BY 
    review_date, review_hour
ORDER BY 
    review_date, review_hour;

SELECT 
    review_date,
    AVG(event_count / time_spent) OVER (ORDER BY review_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS avg_throughput
FROM (
    SELECT 
        DATE_FORMAT(STR_TO_DATE(ds, '%m/%d/%Y'), '%Y-%m-%d') AS review_date,
        COUNT(event) AS event_count,
        SUM(time_spent) AS time_spent
    FROM 
        job_data
    GROUP BY 
        review_date
) AS daily_stats
ORDER BY 
    review_date;
    
    use job_analysis;
    select * from job_data;
SELECT ds FROM job_data;
SELECT 
    ds, 
    STR_TO_DATE(ds, '%m/%d/%Y') AS converted_date
FROM 
    job_data;

SELECT 
    COUNT(*) AS total_records_last_30_days
FROM 
    job_data
WHERE 
    STR_TO_DATE(ds, '%m/%d/%Y') >= DATE_SUB(CURDATE(), INTERVAL 1600 DAY);
    
SELECT 
    language,
    ROUND((COUNT(*) / (SELECT 
                    COUNT(*)
                FROM
                    job_data
                WHERE
                    STR_TO_DATE(ds, '%m/%d/%Y') >= DATE_SUB(CURDATE(), INTERVAL 1600 DAY))) * 100,
            2) AS language_share
FROM
    job_data
WHERE
    STR_TO_DATE(ds, '%m/%d/%Y') >= DATE_SUB(CURDATE(), INTERVAL 1600 DAY)
GROUP BY language
ORDER BY language_share DESC;


SELECT 
    ds,
    job_id,
    actor_id,
    event,
    language,
    time_spent,
    org,
    COUNT(*) AS duplicate_count
FROM
    job_data
GROUP BY ds , job_id , actor_id , event , language , time_spent , org
HAVING duplicate_count > 1;


use job_analysis;
create table users(
user_id	int,
created_at	varchar(100),
company_id int,
language varchar(50),
activated_at varchar(100),
state varchar(50)
);
select * from users;

alter table users add column temp_created_at datetime;
UPDATE users SET temp_created_at = STR_TO_DATE(created_at, '%d-%m-%Y %H:%i');
ALTER TABLE users DROP COLUMN created_at;
ALTER TABLE users CHANGE COLUMN temp_created_at created_at DATETIME;

alter table users add column temp_activated_at datetime;
UPDATE users SET temp_activated_at = STR_TO_DATE(activated_at, '%d-%m-%Y %H:%i');
ALTER TABLE users DROP COLUMN activated_at;
ALTER TABLE users CHANGE COLUMN temp_activated_at activated_at DATETIME;

CREATE TABLE events (
user_id INT,
occurred_at VARCHAR(100), 
event_type VARCHAR(50),
event_name VARCHAR(100), 
location VARCHAR(58), 
device VARCHAR(50), 
user_type INT
);

use job_analysis;
alter table events add column temp_occured_at datetime;
UPDATE events SET temp_occured_at = STR_TO_DATE(occurred_at, '%d-%m-%Y %H:%i');
ALTER TABLE events DROP COLUMN occurred_at;
ALTER TABLE events CHANGE COLUMN temp_occured_at occurred_at DATETIME;
select * from events;

CREATE TABLE email_events (	
user_id INT,
occurred_at VARCHAR(100), 
action VARCHAR(100),
user_type INT
);

select * from email_events;
alter table email_events add column temp_occurred_at datetime;
UPDATE email_events SET temp_occurred_at = STR_TO_DATE(occurred_at, '%d-%m-%Y %H:%i');
ALTER TABLE email_events DROP COLUMN occurred_at;
ALTER TABLE email_events CHANGE COLUMN temp_occurred_at occurred_at DATETIME;


SELECT 
    YEARWEEK(occurred_at, 1) AS week,
    COUNT(DISTINCT user_id) AS weekly_active_users
FROM 
    events
WHERE 
    event_type = 'engagement'
GROUP BY 
    YEARWEEK(occurred_at, 1);


SELECT 
    YEARWEEK(created_at, 1) AS week,
    COUNT(user_id) AS new_users,
    SUM(COUNT(user_id)) OVER (ORDER BY YEARWEEK(created_at, 1)) AS cumulative_users
FROM 
    users
GROUP BY 
    YEARWEEK(created_at, 1);
    
SELECT 
    YEARWEEK(u.created_at, 1) AS signup_week,
    YEARWEEK(e.occurred_at, 1) AS active_week,
    COUNT(DISTINCT u.user_id) AS retained_users
FROM 
    users u
JOIN 
    events e ON u.user_id = e.user_id
WHERE 
    e.event_type = 'engagement' 
    AND YEARWEEK(e.occurred_at, 1) > YEARWEEK(u.created_at, 1)
GROUP BY 
    signup_week, active_week
ORDER BY 
    signup_week, active_week;
    
SELECT 
    YEARWEEK(occurred_at, 1) AS week,
    device,
    COUNT(DISTINCT user_id) AS weekly_active_users
FROM 
    events
WHERE 
    event_type = 'engagement'
GROUP BY 
    week, device;

SELECT 
    YEARWEEK(occurred_at, 1) AS week,
    COUNT(CASE WHEN action = 'sent_weekly_digest' THEN 1 END) AS emails_sent,
    COUNT(CASE WHEN action = 'email_open' THEN 1 END) AS emails_opened,
    COUNT(DISTINCT CASE WHEN action = 'email_open' THEN user_id END) AS unique_email_opens
FROM 
    email_events
GROUP BY 
    week;



