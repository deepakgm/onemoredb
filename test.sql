SELECT n.n_nationkey
FROM nation AS n, region AS r, customer AS c
WHERE (n.n_regionkey = r.n_regionkey) AND (n.n_nationkey = c.c_nationkey) AND (n.n_nationkey > 10)