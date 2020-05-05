## OneMoreDB

This is a relational database created as a part of the academic project for the course [Database System Implementation](https://www.cise.ufl.edu/class/cop6726sp15/index) at the University of Florida.

To compile:
```shell script
make clean
make a5.out
```

To execute:
```shell script
./a5.out
```
Enter the query then press Cntrl+D

Sample Query 
```sql
SELECT SUM DISTINCT (n.n_nationkey + r.r_regionkey) FROM nation AS n, region AS r, customer AS c WHERE (n.n_regionkey = r.r_regionkey) AND (n.n_nationkey = c.c_nationkey) AND (n.n_nationkey > 10) GROUP BY r.r_regionkey
```

Following Operations are supported:
- CREATE TABLE
- DROP TABLE
- INSERT
- SELECT

##### TODO:
* Implement delete operation for bplus tree based storage
* Fix bugs in query optimizer
* Support for 'Distinct' operation for queries not having 'GroupBy' operator 



#####Thanks to:

* [Prof. Alin Dobra](https://www.cise.ufl.edu/~adobra/)
* All the TAs for the course
* Following git repos have inspired us a lot during the project:
    + https://github.com/zcbenz/BPlusTree
    + https://github.com/MorganBauer/COP-6726-Databases-Project/
    + https://github.com/zmcwu/COP6726-Project
     

