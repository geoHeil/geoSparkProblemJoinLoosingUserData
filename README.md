# geospark join problem
the join works fine, however the userData field of the joined `(Polygon, util.HashSet[Point])` is empty. What is wrong here?

This is a minimal example which can be run as

```
sbt run
```

or 
```
sbt test
```
to show the problems.

**problem description**
instead of seeing joined results for the last field (the user data field)
```
+---+---+---+---------+
|  a|  b|  c|        d|
+---+---+---+---------+
|  A|  A|123|      [A]|
|  A|  A|123|      [B]|
|  B|  B|123|   [C, A]|
|  B|  B|123|[C, E, A]|
|  B|  B|123|      [D]|
+---+---+---+---------+
```
it is empty
```
+---+---+---+---------+
|  a|  b|  c|        d|
+---+---+---+---------+
|  A|  A|123|      [] |
|  A|  A|123|      [] |
|  B|  B|123|   []    |
|  B|  B|123|[]       |
|  B|  B|123|      [] |
+---+---+---+---------+
```
Unfortunately, so far I could not reproduce this with some hand drawn / made up data (yet). Still maybe you already had problems like that?

During the development of this minimal example I noticed in case of duplicate Points (still with different IDs) only a single one is joined (if at all). How can this be explained?
```
+---------+----------------+----------------+
|       id|        latitude|       longitude|
+---------+----------------+----------------+
|600000001| 5.4588922996667| 5.4588922996667|
|6000000SS| 5.4588922996667| 5.4588922996667|
|600000033| 5.4588922996667| 5.4588922996667|
|600000000| 5.4588922996667| 5.4588922996667|
|600000002| 5.4588922996667| 5.4588922996667|
|6000000SS| 5.4588922996667| 5.4588922996667|
|100000000|15.4588922996667|47.0500470991844|
|100000001|15.4427295917601|47.0628953964286|
+---------+----------------+----------------+

+---------+-------+--------------------+
|      id1|idValue|           wktString|
+---------+-------+--------------------+
|location2|    456|POLYGON ((0.45889...|
|      foo|    -70|MULTIPOLYGON (((1...|
+---------+-------+--------------------+

join count 1
join RDD count 1
17/03/20 17:27:41 WARN Executor: 1 block locks were not released by TID = 33:
[rdd_42_1]
600000001
join size 1
+---------+---+-----------+
|        a|  c|   pointIds|
+---------+---+-----------+
|location2|456|[600000001]|
+---------+---+-----------+
```
i.e. the points
```
foo,16.52805721081677,47.850179651896994
second,16.529602163209347,47.85023725039687
```
clearly should be contained in the multi polygon of 

```
POLYGON((16.5132943323988 47.85121641510742,16.523422353639035 47.83923477616023,16.558183782472042 47.84741484867311,16.558183782472042 47.86106463188969,16.53106128491345 47.865210689885984,16.52951633252087 47.85565122375945,16.516985052003292 47.857378968615265,16.5132943323988 47.85121641510742))
```

but when manually altering the points i.e. just changing the last digit, then multiple fields are shown. This is not the desired behaviour, as for me the labels should be taken into consideration when deciding on uniqueness of the points.

```
5.4588922996669,5.4588922996669
5.4588922996668,5.4588922996668
5.4588922996667,5.4588922996667
```
# magellan comparison

whereas geospark will find atleast a single join, magellan will return no joins at all. What is wrong here?
```
+----+--------+---------+-----+--------+----+---+---------+-------+
|upac|latitude|longitude|point|sitename|neId| db|wktString|polygon|
+----+--------+---------+-----+--------+----+---+---------+-------+
+----+--------+---------+-----+--------+----+---+---------+-------+
+----+--------+---------+-----+--------+----+---+---------+-------+
```