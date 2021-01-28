# Vertica DataSketches
This repository contains C++ bindings between Apache Datasketches library and Vertica Database. It was created by the Analytics Infrastructure teams at Criteo.

Details on the library and underlying algorithm can be found here https://datasketches.apache.org/

This extensions uses the open-source C++ implementation https://github.com/apache/incubator-datasketches-cpp/

**Currently only the theta sketch, Hll sketch, and frequency sketch are implemented for Vertica, see examples below.**

## Install
This library requires cmake 3.14+  "yum install cmake3" package should install the correct version

```
mkdir build
cd build
cmake ../SOURCES
make
```

Additional build options can be enabled by runing ccmake.

To install, copy the library and SOURCES/install.sql to a Vertica node.  Edit install.sql and copy the correct library path and file name at the top, then run with `vsql -f install.sql`

## Examples
Theta sketch is used to estimate cardinality.  Frequency sketch is used to estimate most common items.  Consider the following table and sketches:
```
dbadmin=> select * from freq order by v1;
 v1
----
 a
 a
 a
 b
 b
 c
(6 rows)

dbadmin=> select theta_sketch_get_estimate(theta_sketch_create(v1)) from freq;
 theta_sketch_get_estimate
---------------------------
                         3
(1 row)

dbadmin=> select hll_sketch_create(v1) from freq;
 hll_sketch_create
-------------------
                 3
(1 row)

dbadmin=> select frequency_sketch_create(v1) from freq;
 frequency_sketch_create
-------------------------
 [[a,3],[b,2],[c,1]]
(1 row)
```
## Known issues
In Vertica, each query is given at runtime a pool which depends of the configuration of the database and the context (User, Roles, etc).

The Datasketch-CPP library uses C++ standard allocators to allocate/release the memory required for sketch processing.

The problem is that in its current state, the Datasketch library can only be integrated with compile time/ static allocators and the API does not offer a way to initialize those allocators with external resource at runtime (calls to allocators default constructor internally).

Ideally the datasketch library would allow users to pass in instances of custom allocator rather than only their types.

As a workaround we have built a simple custom memory allocator that constrains the algorithm up to 10GB of memory (of heap outside of the vertica pool).

**This is not ideal** and we plan to improve that by working the the datasketches-cpp maintainers.
