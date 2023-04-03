-- Sakila Database is a database from a rental videoclub
-- Yes, streaming did not always exist. We needed physical movies and physical playback devices.

#1 How many customers does the videoclub has?
SELECT DISTINCT COUNT(customer_id) AS UniqueCustomers
FROM sakila.payment;

#2 Average Spending of Customers
SELECT customer_id, AVG(amount) AS avg_spent
FROM sakila.payment
GROUP BY customer_id
ORDER BY 2 DESC;


#3 How many cities from Argentina do we have in our database?
SELECT COUNT(*) AS num_cities
FROM sakila.city
WHERE country_id = (SELECT country_id FROM sakila.country WHERE country = 'Argentina');

#4 Show me the cities
SELECT co.country, c.city
FROM sakila.city AS c
INNER JOIN sakila.country AS co
ON co.country_id = c.country_id
WHERE co.country = 'Argentina';


#5 Divide Movie Rental Rate in 3 Bins
SELECT title, rental_rate,
CASE
 WHEN rental_rate < 1 THEN "Bad Movie"
 WHEN rental_rate BETWEEN 1 AND 3 THEN "Good Movie"
 ELSE "Excellent Movie"
 END AS "Qualification"
FROM sakila.film;


#6 Staff member with more sales August 2005
SELECT s.first_name, s.last_name, SUM(p.amount) AS TotalAmount
FROM sakila.staff AS s
INNER JOIN sakila.payment AS p
ON s.staff_id = p.staff_id AND p.payment_date LIKE '2005-08%'
GROUP BY 1,2
ORDER BY 3 DESC;

#8 How many copies of Hunchback Impossible movie are in the inventory?
SELECT title, COUNT(title)
FROM sakila.film AS f
INNER JOIN sakila.inventory AS i
ON f.film_id = i.film_id
WHERE title = 'Hunchback Impossible'
