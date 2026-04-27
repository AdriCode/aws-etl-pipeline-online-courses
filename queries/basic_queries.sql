-- 1) Total courses by platform
SELECT platform, COUNT(*) as total_courses
FROM "online_courses_db"."processed_fact"
GROUP BY platform
ORDER BY total_courses DESC;

-- 2) Top 10 subjects by enrollment
SELECT subject,
       SUM(num_subscribers) as total_enrollments,
       COUNT(*) as num_courses
FROM "online_courses_db"."processed_fact"
WHERE subject IS NOT NULL
GROUP BY subject
ORDER BY total_enrollments DESC
LIMIT 10;

-- 3) Yearly course publication trend
SELECT published_year,
       COUNT(*) as courses_published
FROM "online_courses_db"."processed_fact"
WHERE published_year IS NOT NULL
  AND published_year BETWEEN 2010 AND 2024
GROUP BY published_year
ORDER BY published_year ASC;

-- 4)  Top 10 courses by enrollment
SELECT course_title, platform, subject,
       num_subscribers, course_rating
FROM "online_courses_db"."processed_fact"
WHERE num_subscribers IS NOT NULL
ORDER BY num_subscribers DESC
LIMIT 10;
