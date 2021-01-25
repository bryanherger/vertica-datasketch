#include <Vertica.h>
#include <theta_sketch.hpp>
#include "../../../include/datasketches/theta/theta_common.hpp"

using namespace Vertica;

class ThetaSketchLBound : public ScalarFunction {
public:
    void processBlock(ServerInterface &srvInterface,
                      BlockReader &argReader,
                      BlockWriter &resWriter) {
        try {
            auto seed = readSeed(srvInterface);
            const SizedColumnTypes &inTypes = argReader.getTypeMetaData();
            std::vector<size_t> argCols; // Argument column indexes.
            inTypes.getArgumentColumns(argCols);
            // While we have inputs to process
            do {
                auto sketch = datasketches::compact_theta_sketch::deserialize(argReader.getStringRef(0).data(),
                                                                              argReader.getStringRef(0).length(),
                                                                              seed);
                resWriter.setFloat(sketch.get_lower_bound(argReader.getIntRef(1)));
                resWriter.next();
            } while (argReader.next());
        } catch (std::exception &e) {
            // Standard exception. Quit.
            vt_report_error(0, "Exception while processing block: [%s]", e.what());
        }
    }
};

class ThetaSketchGetLBoundFactory : public ScalarFunctionFactory {
    // return an instance of AddAnyInts to perform the actual addition.
    virtual ScalarFunction *createScalarFunction(ServerInterface &interface) {
        return vt_createFuncObject<ThetaSketchLBound>(interface.allocator);
    }

    virtual void getPrototype(ServerInterface &interface,
                              ColumnTypes &argTypes,
                              ColumnTypes &returnType) {
        argTypes.addLongVarbinary();
        argTypes.addInt();
        returnType.addFloat();
    }

    virtual void getParameterType(ServerInterface &srvInterface,
                                  SizedColumnTypes &parameterTypes) {
        SizedColumnTypes::Properties seedProps;
        seedProps.required = false;
        seedProps.canBeNull = false;
        seedProps.comment = "Seed value";
        parameterTypes.addInt(DATASKETCHES_SEED_PARAMETER_NAME, seedProps);
    }
};

class ThetaSketchUBound : public ScalarFunction {
public:
    void processBlock(ServerInterface &srvInterface,
                      BlockReader &argReader,
                      BlockWriter &resWriter) {
        try {
            auto seed = readSeed(srvInterface);
            const SizedColumnTypes &inTypes = argReader.getTypeMetaData();
            std::vector<size_t> argCols; // Argument column indexes.
            inTypes.getArgumentColumns(argCols);
            // While we have inputs to process
            do {
                auto sketch = datasketches::compact_theta_sketch::deserialize(argReader.getStringRef(0).data(),
                                                                              argReader.getStringRef(0).length(),
                                                                              seed);
                resWriter.setFloat(sketch.get_upper_bound(argReader.getIntRef(1)));
                resWriter.next();
            } while (argReader.next());
        } catch (std::exception &e) {
            // Standard exception. Quit.
            vt_report_error(0, "Exception while processing block: [%s]", e.what());
        }
    }
};

class ThetaSketchGetUBoundFactory : public ScalarFunctionFactory {
    // return an instance of AddAnyInts to perform the actual addition.
    virtual ScalarFunction *createScalarFunction(ServerInterface &interface) {
        return vt_createFuncObject<ThetaSketchUBound>(interface.allocator);
    }

    virtual void getPrototype(ServerInterface &interface,
                              ColumnTypes &argTypes,
                              ColumnTypes &returnType) {
        argTypes.addLongVarbinary();
        argTypes.addInt();
        returnType.addFloat();
    }

    virtual void getParameterType(ServerInterface &srvInterface,
                                  SizedColumnTypes &parameterTypes) {
        SizedColumnTypes::Properties seedProps;
        seedProps.required = false;
        seedProps.canBeNull = false;
        seedProps.comment = "Seed value";
        parameterTypes.addInt(DATASKETCHES_SEED_PARAMETER_NAME, seedProps);
    }
};

RegisterFactory(ThetaSketchGetLBoundFactory);
RegisterFactory(ThetaSketchGetUBoundFactory);
