#include "Vertica.h"
#include <iostream>
#include <thread>
#include <frequent_items_sketch.hpp>

using namespace Vertica;
using namespace std;

typedef datasketches::frequent_items_sketch<std::string> frequent_strings_sketch;

uint8_t readTopK(ServerInterface &serverInterface) {
    vint topK;
    ParamReader paramReader = serverInterface.getParamReader();

    if (paramReader.containsParameter("topK")) {
        topK = paramReader.getIntRef("topK");
        if (topK < 1 || topK > 1000000) {
            vt_report_error(2,
                            "Provided value of the %s parameter is not supported. The value should be between %d and %d, inclusive",
                            "topK", 1,
                            1000000);
        }
    } else {
        LogDebugUDxWarn(serverInterface, "Parameter %s was not provided. Defaulting to %d",
                        "topK", 1000);
        topK = 1000;
    }
    return topK;
}

/**
 * User Defined Aggregate Function concatenate that implements the frequent_items_sketch
 * Based on example from https://datasketches.apache.org/docs/Frequency/FrequentItemsCppExample.html
 */
class FrequencyAggregateCreate : public AggregateFunction {
protected:
    uint8_t topK;

public:
    virtual void setup(ServerInterface &srvInterface, const SizedColumnTypes &argTypes) {
        this->topK = readTopK(srvInterface);
    }

    virtual void initAggregate(ServerInterface &srvInterface, IntermediateAggs &aggs) {
        try {
            frequent_strings_sketch updatex(topK);
            auto data = updatex.serialize(); // provides compact & rebuild sketch <=> min size
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
            const VString &concat = aggs.getStringRef(0);
            VString &result = resWriter.getStringRef();
            frequent_strings_sketch u = frequent_strings_sketch::deserialize(aggs.getStringRef(0).data(),aggs.getStringRef(0).length());
            auto items = u.get_frequent_items(datasketches::NO_FALSE_POSITIVES);
            ostringstream os;
            //os << "Frequent strings:" << items.size() << "|";
            os << "[";
            bool pastFirst = false;
            for (auto row: items) {
                if (pastFirst) {
                    os << ",";
                } else {
                    pastFirst = true;
                }
                os << "[" << row.get_item() << "," << row.get_estimate() << "]";
            }
            os << "]";
            result.copy(os.str());
        } catch (exception &e) {
            // Standard exception. Quit.
            vt_report_error(0, "Exception while computing aggregate output: [%s]", e.what());
        }
    }

    void aggregate(ServerInterface &srvInterface,
                   BlockReader &argReader,
                   IntermediateAggs &aggs) {
        try {
            frequent_strings_sketch updatex = frequent_strings_sketch::deserialize(aggs.getStringRef(0).data(),aggs.getStringRef(0).length());
            do {
                updatex.update(argReader.getStringRef(0).str());
            } while (argReader.next());
            srvInterface.log("a:%d", updatex.get_frequent_items(datasketches::NO_FALSE_POSITIVES).size());
            auto data = updatex.serialize();
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
            frequent_strings_sketch u = frequent_strings_sketch::deserialize(aggs.getStringRef(0).data(),aggs.getStringRef(0).length());
            srvInterface.log("c:u:%d", u.get_frequent_items(datasketches::NO_FALSE_POSITIVES).size());

            do {
                frequent_strings_sketch um = frequent_strings_sketch::deserialize(aggsOther.getStringRef(0).data(),aggsOther.getStringRef(0).length());
                srvInterface.log("c:um:%d", um.get_frequent_items(datasketches::NO_FALSE_POSITIVES).size());
                u.merge(um);
                srvInterface.log("c:merge:%d", u.get_frequent_items(datasketches::NO_FALSE_POSITIVES).size());
            } while (aggsOther.next());

            srvInterface.log("c:final:%d", u.get_frequent_items(datasketches::NO_FALSE_POSITIVES).size());
            auto data = u.serialize();
            aggs.getStringRef(0).copy((char *)&data[0], data.size());

        } catch (exception &e) {
            // Standard exception. Quit.
            vt_report_error(0, "Exception while combining intermediate aggregates: [%s]", e.what());
        }
    }

    InlineAggregate()
};

class FrequencyAggregateCreateFactory : public AggregateFunctionFactory {
    virtual void getPrototype(ServerInterface &srvfloaterface, ColumnTypes &argTypes, ColumnTypes &returnType) {
        argTypes.addVarchar();
        returnType.addLongVarchar();
    }
    virtual void getIntermediateTypes(ServerInterface &srvInterface,
                                      const SizedColumnTypes &inputTypes,
                                      SizedColumnTypes &intermediateTypeMetaData) {
        intermediateTypeMetaData.addLongVarbinary(320000);
    }

    virtual void getReturnType(ServerInterface &srvfloaterface,
                               const SizedColumnTypes &inputTypes,
                               SizedColumnTypes &outputTypes) {
        outputTypes.addLongVarchar(500000);
    }

    virtual void getParameterType(ServerInterface &srvInterface,
                                  SizedColumnTypes &parameterTypes) {
        SizedColumnTypes::Properties logNominalProps;
        logNominalProps.required = false;
        logNominalProps.canBeNull = false;
        logNominalProps.comment = "Log Nominal value.";
        parameterTypes.addInt("topK", logNominalProps);
    }

    virtual AggregateFunction *createAggregateFunction(ServerInterface &srvInterface) {
        return vt_createFuncObject<FrequencyAggregateCreate>(srvInterface.allocator);
    }
};

RegisterFactory(FrequencyAggregateCreateFactory);
