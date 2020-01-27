/* most rated movies:
    - with an average rating of at least 4.0
    - having at least 50 ratings
*/

SELECT
	r.movieid,
	m.title,
	AVG(r.rating),
	COUNT(r.rating)
FROM ratings r
INNER JOIN movies_metadata m
	ON r.movieid = m.id
GROUP BY r.movieid, m.title
HAVING
	AVG(r.rating) >= 4.0
	AND COUNT(r.rating) >= 50
ORDER BY AVG(r.rating) DESC

