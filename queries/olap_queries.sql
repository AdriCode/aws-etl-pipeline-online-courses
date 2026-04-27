-- 1) Roll-up: Enrollments from subject → platform → grand total
SELECT 
    COALESCE(platform, 'ALL PLATFORMS') as platform,
    COALESCE(subject, 'ALL SUBJECTS') as subject,
    COUNT(*) as total_courses,
    SUM(num_subscribers) as total_enrollments,
    ROUND(AVG(course_rating), 2) as avg_rating
FROM "online_courses_db"."processed_fact"
WHERE num_subscribers IS NOT NULL
GROUP BY ROLLUP(platform, subject)
ORDER BY platform, subject;

-- 2)  Dice: Slice by platform AND price category AND rating > 4
SELECT 
    platform,
    price_category,
    level,
    COUNT(*) as total_courses,
    ROUND(AVG(course_rating), 2) as avg_rating,
    SUM(num_subscribers) as total_enrollments
FROM "online_courses_db"."processed_fact"
WHERE course_rating > 4.0
  AND price_category IS NOT NULL
  AND platform IS NOT NULL
GROUP BY platform, price_category, level
ORDER BY avg_rating DESC;

-- 3) Ranking: Top subjects using RANK() window function
SELECT 
    platform,
    subject,
    total_enrollments,
    avg_rating,
    RANK() OVER (PARTITION BY platform ORDER BY total_enrollments DESC) as enrollment_rank,
    RANK() OVER (PARTITION BY platform ORDER BY avg_rating DESC) as rating_rank
FROM (
    SELECT 
        platform,
        subject,
        SUM(num_subscribers) as total_enrollments,
        ROUND(AVG(course_rating), 2) as avg_rating
    FROM "online_courses_db"."processed_fact"
    WHERE subject IS NOT NULL AND num_subscribers IS NOT NULL
    GROUP BY platform, subject
) ranked
ORDER BY platform, enrollment_rank;

-- 4) Pivot-style: Price category vs platform comparison
SELECT
    subject,
    SUM(CASE WHEN platform = 'Udemy' THEN num_subscribers ELSE 0 END) as udemy_enrollments,
    SUM(CASE WHEN platform = 'Coursera' THEN num_subscribers ELSE 0 END) as coursera_enrollments,
    ROUND(AVG(CASE WHEN platform = 'Udemy' THEN course_rating END), 2) as udemy_avg_rating,
    ROUND(AVG(CASE WHEN platform = 'Coursera' THEN course_rating END), 2) as coursera_avg_rating
FROM "online_courses_db"."processed_fact"
WHERE subject IS NOT NULL AND num_subscribers IS NOT NULL
GROUP BY subject
ORDER BY udemy_enrollments DESC
LIMIT 15;

-- 5) Percentile analysis using window functions
SELECT
    platform,
    subject,
    course_title,
    num_subscribers,
    course_rating,
    NTILE(4) OVER (PARTITION BY platform ORDER BY num_subscribers DESC) as enrollment_quartile,
    NTILE(4) OVER (PARTITION BY platform ORDER BY course_rating DESC) as rating_quartile
FROM "online_courses_db"."processed_fact"
WHERE num_subscribers IS NOT NULL
  AND course_rating IS NOT NULL
ORDER BY platform, enrollment_quartile;
