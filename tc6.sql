SELECT SUM (ps.ps_supplycost), statistics.s_suppkey
FROM part AS p, partsupp AS ps, supplier AS statistics
WHERE (p.p_partkey = ps.ps_partkey) AND (statistics.s_suppkey = ps.ps_suppkey) AND (statistics.s_acctbal > 2500.0)
GROUP BY statistics.s_suppkey
