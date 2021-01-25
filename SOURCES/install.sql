\set ON_ERROR_STOP on
DROP LIBRARY IF EXISTS DataSketches CASCADE;

CREATE OR REPLACE LIBRARY DataSketches AS '/home/dbadmin/github/vertica-datasketch/build/libvertica-datasketches.so';

create or replace transform function theta_sketch_create_udtf as language 'C++' name 'ThetaCreateUDTFFactory' library DataSketches;
GRANT EXECUTE ON TRANSFORM FUNCTION theta_sketch_create_udtf(VARCHAR) TO PUBLIC;

-- SELECT theta_sketch_get_estimate(theta_sketch) FROM ...
CREATE OR REPLACE FUNCTION theta_sketch_get_estimate AS
    LANGUAGE 'C++'
    NAME 'ThetaSketchGetEstimateFactory' LIBRARY DataSketches;
GRANT EXECUTE ON FUNCTION theta_sketch_get_estimate(LONG VARBINARY) TO PUBLIC;

-- SELECT theta_sketch_get_lower_bound(theta_sketch, kappa) FROM ...
-- "kappa - the given number of standard deviations from the mean: 1, 2 or 3."
CREATE OR REPLACE FUNCTION theta_sketch_get_lower_bound AS
    LANGUAGE 'C++'
    NAME 'ThetaSketchGetLBoundFactory' LIBRARY DataSketches;
GRANT EXECUTE ON FUNCTION theta_sketch_get_lower_bound(LONG VARBINARY, INTEGER) TO PUBLIC;

-- SELECT theta_sketch_get_upper_bound(theta_sketch, kappa) FROM ...
-- "kappa - the given number of standard deviations from the mean: 1, 2 or 3."
CREATE OR REPLACE FUNCTION theta_sketch_get_upper_bound AS
    LANGUAGE 'C++'
    NAME 'ThetaSketchGetUBoundFactory' LIBRARY DataSketches;
GRANT EXECUTE ON FUNCTION theta_sketch_get_upper_bound(LONG VARBINARY, INTEGER) TO PUBLIC;

-- SELECT theta_sketch_union(theta_sketch1, theta_sketch2, ...) FROM ...
CREATE OR REPLACE FUNCTION theta_sketch_union AS
    LANGUAGE 'C++'
    NAME 'ThetaSketchScalarUnionFactory' LIBRARY DataSketches;
GRANT EXECUTE ON FUNCTION theta_sketch_union(LONG VARBINARY) TO PUBLIC;

-- SELECT key, theta_sketch_union_agg(theta_sketch) FROM ... GROUP BY key
CREATE OR REPLACE AGGREGATE FUNCTION theta_sketch_union_agg AS
    LANGUAGE 'C++'
    NAME 'ThetaSketchAggregateUnionFactory' LIBRARY DataSketches;
GRANT EXECUTE ON AGGREGATE FUNCTION theta_sketch_union_agg(LONG VARBINARY) TO PUBLIC;

-- SELECT key, theta_sketch_create(binary) FROM ... GROUP BY key
CREATE OR REPLACE AGGREGATE FUNCTION theta_sketch_create AS
    LANGUAGE 'C++'
    NAME 'ThetaSketchAggregateCreateVarcharFactory' LIBRARY DataSketches;
GRANT EXECUTE ON AGGREGATE FUNCTION theta_sketch_create(VARCHAR) TO PUBLIC;

-- SELECT key, theta_sketch_create(chars) FROM ... GROUP BY key
CREATE OR REPLACE AGGREGATE FUNCTION theta_sketch_create AS
    LANGUAGE 'C++'
    NAME 'ThetaSketchAggregateCreateVarbinaryFactory' LIBRARY DataSketches;
GRANT EXECUTE ON AGGREGATE FUNCTION theta_sketch_create(VARBINARY) TO PUBLIC;

-- SELECT theta_sketch_intersection(theta_sketch1, theta_sketch2, ...) FROM ...
CREATE OR REPLACE FUNCTION theta_sketch_intersection AS
    LANGUAGE 'C++'
    NAME 'ThetaSketchScalarIntersectionFactory' LIBRARY DataSketches;
GRANT EXECUTE ON FUNCTION theta_sketch_intersection(LONG VARBINARY) TO PUBLIC;

-- SELECT key, theta_sketch_intersection_agg(theta_sketch) FROM ... GROUP BY key
CREATE OR REPLACE AGGREGATE FUNCTION theta_sketch_intersection_agg AS
    LANGUAGE 'C++'
    NAME 'ThetaSketchAggregateIntersectionFactory' LIBRARY DataSketches;
GRANT EXECUTE ON AGGREGATE FUNCTION theta_sketch_intersection_agg(LONG VARBINARY) TO PUBLIC;

-- SELECT theta_sketch_a_not_b(theta_sketch_a, theta_sketch_b) FROM ...
CREATE OR REPLACE FUNCTION theta_sketch_a_not_b AS
    LANGUAGE 'C++'
    NAME 'ThetaSketchANotBFactory' LIBRARY DataSketches;
GRANT EXECUTE ON FUNCTION theta_sketch_a_not_b(LONG VARBINARY, LONG VARBINARY) TO PUBLIC;
