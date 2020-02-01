CREATE VIEW most_popular_movies AS
SELECT
    movieid,
    AVG(rating)     AS avg_rating,
    COUNT(rating)   AS count_rating
FROM movieratings
GROUP BY movieid
HAVING
	AVG(rating) >= 4.0
	AND COUNT(rating) >= 500;

SELECT
	r.movieid,
	m.title,
	r.avg_rating,
	r.count_rating
FROM most_popular_movies r
INNER JOIN moviesmetadata m
	ON r.movieid = m.id
ORDER BY r.avg_rating DESC;

-------------------
SELECT
	r.movieid,
	m.title,
	AVG(r.rating),
	COUNT(r.rating)
FROM movieratings r
INNER JOIN moviesmetadata m
	ON r.movieid = m.id
GROUP BY r.movieid, m.title
HAVING
	AVG(r.rating) >= 4.0
	AND COUNT(r.rating) >= 50;