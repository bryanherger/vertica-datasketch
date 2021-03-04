#include "Vertica.h"
#include <iostream>
#include <thread>
#include <theta_sketch.hpp>
#include <theta_union.hpp>
#include "../../../include/datasketches/theta/theta_common.hpp"

using namespace Vertica;
using namespace std;

/**
 * User Defined Aggregate Function concatenate that takes in strings and concatenates
 * them together. Right now, the max length of the resulting string is ten times the
 * maximum length of the input string.
 */
class ThetaSketchAggregateCreate : public ThetaSketchAggregateFunction {
    update_theta_sketch_custom updatex = update_theta_sketch_custom::builder().build();

    virtual void initAggregate(ServerInterface &srvInterface, 
                               IntermediateAggs &aggs)
    {
        try {
            updatex = update_theta_sketch_custom::builder().set_lg_k(logK).set_seed(seed).build();
            auto data = updatex.compact().serialize();
            aggs.getStringRef(0).copy((char *) &data[0], data.size());
        } catch (exception &e) {
            // Standard exception. Quit.
            vt_report_error(0, "Exception while initializing intermediate aggregates: [%s]", e.what());
        }
    }

    void aggregate(ServerInterface &srvInterface,
                   BlockReader &argReader,
                   IntermediateAggs &aggs) {
        try {
            do {
                updatex.update(argReader.getStringRef(0).str());
            } while (argReader.next());
            auto data = updatex.compact().serialize();
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
            auto u = theta_union_custom::builder()
                    .set_lg_k(logK)
                    .set_seed(seed)
                    .build();

            auto sketch = compact_theta_sketch_custom::deserialize(aggs.getStringRef(0).data(),
                                                            aggs.getStringRef(0).length(),
                                                            seed);
            u.update(sketch);

            do {
                sketch = compact_theta_sketch_custom::deserialize(aggsOther.getStringRef(0).data(),
                                                           aggsOther.getStringRef(0).length(),
                                                           seed);
                u.update(sketch);
            } while (aggsOther.next());

            auto data = u.get_result().serialize();
            aggs.getStringRef(0).copy((char *)&data[0], data.size());

        } catch (exception &e) {
            // Standard exception. Quit.
            vt_report_error(0, "Exception while combining intermediate aggregates: [%s]", e.what());
        }
    }

    InlineAggregate()
};


class ThetaSketchAggregateCreateVarcharFactory : public ThetaSketchAggregateFunctionFactory {
    virtual void getPrototype(ServerInterface &srvfloaterface, ColumnTypes &argTypes, ColumnTypes &returnType) {
        argTypes.addVarchar();
        returnType.addLongVarbinary();
    }

    virtual AggregateFunction *createAggregateFunction(ServerInterface &srvfloaterface) {
        return vt_createFuncObject<ThetaSketchAggregateCreate>(srvfloaterface.allocator);
    }
};


class ThetaSketchAggregateCreateVarbinaryFactory : public ThetaSketchAggregateFunctionFactory {
    virtual void getPrototype(ServerInterface &srvfloaterface, ColumnTypes &argTypes, ColumnTypes &returnType) {
        argTypes.addVarbinary();
        returnType.addLongVarbinary();
    }

    virtual AggregateFunction *createAggregateFunction(ServerInterface &srvfloaterface) {
        return vt_createFuncObject<ThetaSketchAggregateCreate>(srvfloaterface.allocator);
    }
};

// UDTF
class ThetaCreateUDTF : public TransformFunction
{
protected:
    uint8_t logK;
    uint64_t seed;

public:
    virtual void setup(ServerInterface &srvInterface, const SizedColumnTypes &argTypes) {
        this->logK = readLogK(srvInterface);
        this->seed = readSeed(srvInterface);
    }

  virtual void processPartition(ServerInterface &srvInterface, 
                                PartitionReader &inputReader, 
                                PartitionWriter &outputWriter)
  {
    auto updatex = update_theta_sketch_custom::builder().set_lg_k(logK).set_seed(seed).build();
    int wc = 0;
    try {
      if (inputReader.getNumCols() != 1)
        vt_report_error(0, "Function only accepts 1 argument, but %zu provided", inputReader.getNumCols());

      do {
        //const VString &sentence = inputReader.getStringRef(0);
        updatex.update(inputReader.getStringRef(0).str());
        wc++;
      } while (inputReader.next() && !isCanceled());
      srvInterface.log("UDTF Partition Count %d", wc);
            auto data = updatex.compact().serialize();
            outputWriter.getStringRef(0).copy((char *) &data[0], data.size());
      outputWriter.next();
    } catch(std::exception& e) {
      // Standard exception. Quit.
      vt_report_error(0, "Exception while processing partition: [%s]", e.what());
    }
  }
};

class ThetaCreateUDTFFactory : public TransformFunctionFactory
{
  // Tell Vertica that we take in a row with 1 string, and return a row with 1 string
  virtual void getPrototype(ServerInterface &srvInterface, ColumnTypes &argTypes, ColumnTypes &returnType)
  {
    argTypes.addVarchar();
    returnType.addLongVarbinary();
  }

    virtual void getParameterType(ServerInterface &srvInterface,
                                  SizedColumnTypes &parameterTypes) {
        // Unfortunately it cannot be forced in the intersection...
        // So we have to trust the user to give the maximum logK used of the input sketches.
        SizedColumnTypes::Properties logNominalProps;
        logNominalProps.required = false;
        logNominalProps.canBeNull = false;
        logNominalProps.comment = "Log Nominal value.";
        parameterTypes.addInt(DATASKETCHES_LOG_NOMINAL_VALUE_PARAMETER_NAME, logNominalProps);

        SizedColumnTypes::Properties seedProps;
        seedProps.required = false;
        seedProps.canBeNull = false;
        seedProps.comment = "Seed value";
        parameterTypes.addInt(DATASKETCHES_SEED_PARAMETER_NAME, seedProps);
    }

  // Tell Vertica what our return string length will be, given the input
  // string length
  virtual void getReturnType(ServerInterface &srvInterface, 
                             const SizedColumnTypes &inputTypes, 
                             SizedColumnTypes &outputTypes)
  {
    // Error out if we're called with anything but 1 argument
    if (inputTypes.getColumnCount() != 1)
      vt_report_error(0, "Function only accepts 1 argument, but %zu provided", inputTypes.getColumnCount());

    uint8_t logK = readLogK(srvInterface);
    outputTypes.addLongVarbinary(quickSelectSketchMaxSize(logK), "sketch");
  }

  virtual TransformFunction *createTransformFunction(ServerInterface &srvInterface)
  { return vt_createFuncObject<ThetaCreateUDTF>(srvInterface.allocator); }

};	

RegisterFactory(ThetaCreateUDTFFactory);

RegisterFactory(ThetaSketchAggregateCreateVarcharFactory);
RegisterFactory(ThetaSketchAggregateCreateVarbinaryFactory);

RegisterLibrary(
    "Criteo",// author
    "", // lib_build_tag
    "0.1",// lib_version
    "9.2.1",// lib_sdk_version
    "https://github.com/criteo/vertica-datasketch", // URL
    "Wrapper around incubator-datasketches-cpp to make it usable in Vertica", // description
    "", // licenses required
    ""  // signature
);
