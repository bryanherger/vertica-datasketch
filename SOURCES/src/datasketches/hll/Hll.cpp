#include "Vertica.h"
#include <iostream>
#include <thread>
#include <hll.hpp>
#include "../../../include/datasketches/theta/theta_common.hpp"

using namespace Vertica;
using namespace std;

uint8_t readLogK(ServerInterface &serverInterface);

/**
 * User Defined Aggregate Function concatenate that implements the HyperLogLog sketch
 * Based on example from https://datasketches.apache.org/docs/HLL/HllCppExample.html
 */
class HllAggregateCreate : public AggregateFunction {
protected:
  int logK = 11;
  datasketches::target_hll_type type = datasketches::HLL_4;

public:
    virtual void setup(ServerInterface &srvInterface, const SizedColumnTypes &argTypes) {
        this->logK = readLogK(srvInterface);
    }

    virtual void initAggregate(ServerInterface &srvInterface, IntermediateAggs &aggs) {
        try {
            datasketches::hll_sketch sketch1(logK, type);
            auto data = sketch1.serialize_compact(); // provides compact & rebuild sketch <=> min size
            aggs.getStringRef(0).copy((char *) &data[0], data.size());
        } catch (exception &e) {
            // Standard exception. Quit.
            vt_report_error(0, "Exception while initializing intermediate aggregates: [%s]", e.what());
        }
    }

    virtual void terminate(ServerInterface &srvInterface,
                           BlockWriter &resWriter,
                           IntermediateAggs &aggs) override {
        try {
            datasketches::hll_sketch sketch1 = datasketches::hll_sketch::deserialize(aggs.getStringRef(0).data(),aggs.getStringRef(0).length());
            resWriter.setInt(sketch1.get_estimate());
        } catch (exception &e) {
            // Standard exception. Quit.
            vt_report_error(0, "Exception while computing aggregate output: [%s]", e.what());
        }
    }

    void aggregate(ServerInterface &srvInterface,
                   BlockReader &argReader,
                   IntermediateAggs &aggs) {
        try {
            datasketches::hll_union u(logK);
            datasketches::hll_sketch sketch1 = datasketches::hll_sketch::deserialize(aggs.getStringRef(0).data(),aggs.getStringRef(0).length());
            u.update(sketch1);
            datasketches::hll_sketch sketch2(logK, type);
            do {
                sketch2.update(argReader.getStringRef(0).str());
            } while (argReader.next());
            u.update(sketch2);
            auto data = u.get_result().serialize_compact();
            aggs.getStringRef(0).copy((char *) &data[0], data.size());
        } catch (exception &e) {
            // Standard exception. Quit.
            vt_report_error(0, "Exception while processing aggregate: [%s]", e.what());
        }
    }

    virtual void combine(ServerInterface &srvInterface,
                         IntermediateAggs &aggs,
                         MultipleIntermediateAggs &aggsOther) override {
        try {
            datasketches::hll_union u(logK);
            datasketches::hll_sketch sketch1 = datasketches::hll_sketch::deserialize(aggs.getStringRef(0).data(),aggs.getStringRef(0).length());
            u.update(sketch1);
            do {
                datasketches::hll_sketch sketch2 = datasketches::hll_sketch::deserialize(aggsOther.getStringRef(0).data(),aggsOther.getStringRef(0).length());
                u.update(sketch2);
            } while (aggsOther.next());

            auto data = u.get_result().serialize_compact();
            aggs.getStringRef(0).copy((char *)&data[0], data.size());

        } catch (exception &e) {
            // Standard exception. Quit.
            vt_report_error(0, "Exception while combining intermediate aggregates: [%s]", e.what());
        }
    }

    InlineAggregate()
};

class HllAggregateCreateFactory : public AggregateFunctionFactory {
    virtual void getPrototype(ServerInterface &srvfloaterface, ColumnTypes &argTypes, ColumnTypes &returnType) {
        argTypes.addVarchar();
        returnType.addInt();
    }
    virtual void getIntermediateTypes(ServerInterface &srvInterface,
                                      const SizedColumnTypes &inputTypes,
                                      SizedColumnTypes &intermediateTypeMetaData) {
        intermediateTypeMetaData.addLongVarbinary(320000);
    }

    virtual void getReturnType(ServerInterface &srvfloaterface,
                               const SizedColumnTypes &inputTypes,
                               SizedColumnTypes &outputTypes) {
        outputTypes.addInt();
    }

    virtual void getParameterType(ServerInterface &srvInterface,
                                  SizedColumnTypes &parameterTypes) {
        SizedColumnTypes::Properties logNominalProps;
        logNominalProps.required = false;
        logNominalProps.canBeNull = false;
        logNominalProps.comment = "Log Nominal value.";
        parameterTypes.addInt(DATASKETCHES_LOG_NOMINAL_VALUE_PARAMETER_NAME, logNominalProps);
    }

    virtual AggregateFunction *createAggregateFunction(ServerInterface &srvInterface) {
        return vt_createFuncObject<HllAggregateCreate>(srvInterface.allocator);
    }
};

RegisterFactory(HllAggregateCreateFactory);
