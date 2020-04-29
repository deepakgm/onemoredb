SELECT n.n_name
FROM nation AS n, region AS r
WHERE (n.n_regionkey = r.r_regionkey) AND (n.n_nationkey > 5)


-- SELECT n.n_name FROM nation AS n, region AS r WHERE (n.n_regionkey = r.r_regionkey) AND (n.n_nationkey > 5)

SELECT SUM (n.n_nationkey)
FROM nation AS n, customer AS c
WHERE (n.n_regionkey = c.c_nationkey) AND (n.n_nationkey > 5) GROUP BY n.n_nationkey