-- a) How many racecourses are there in total?
SELECT COUNT(*) AS total_racecourses
FROM tpd_hourse_race.courses;

-- b) What are the horse names, racecourses and post times for horses that appeared in multiple races.
SELECT h.horse_name, c.course_name, r.post_time
FROM tpd_hourse_race.horses h
JOIN tpd_hourse_race.runners ru ON h.horse_id = ru.horse_id
JOIN tpd_hourse_race.races r ON ru.race_id = r.race_id
JOIN tpd_hourse_race.courses c ON r.course_id = c.course_id
WHERE h.horse_id IN (
    SELECT horse_id
    FROM tpd_hourse_race.runners
    GROUP BY horse_id
    HAVING COUNT(DISTINCT race_id) > 1
)
ORDER BY h.horse_name, r.post_time;

-- c) Imagine we wanted to draw a graph showing the number of races per hour of each day. What would the query be and does anything stand out?
SELECT
    DATE(post_time) AS race_date,
    EXTRACT(HOUR FROM post_time) AS race_hour,
    COUNT(*) AS races_count
FROM tpd_hourse_race.races
GROUP BY race_date, race_hour
ORDER BY race_date, race_hour;

-- This query groups races by the date and hour of their post_time, counting the number of races in each hour of each day.
-- What may stand out: Most of races scheduled during the hours : 13 to 20, only one got scheduled at midnight.
