package pl.isa.hadoop;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

public class ConcatUdaf extends AbstractGenericUDAFResolver {
        public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info) throws SemanticException {
                return new Evaluator();
        }

        public static class Evaluator extends GenericUDAFEvaluator {
                private StringObjectInspector soi;

                public static class Buffer implements AggregationBuffer {
                        String value = "";
                }

                public ObjectInspector init(GenericUDAFEvaluator.Mode m, ObjectInspector[] parameters) throws HiveException {
                        super.init(m, parameters);

                        soi = PrimitiveObjectInspectorFactory.javaStringObjectInspector;

                        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
                }

                public AggregationBuffer getNewAggregationBuffer() throws HiveException {
                        return new Buffer();
                }

                public void reset(AggregationBuffer aggregationBuffer) throws HiveException {
                        ((Buffer)aggregationBuffer).value = "";
                }

                public void iterate(AggregationBuffer aggregationBuffer, Object[] objects) throws HiveException {
                        String val = soi.getPrimitiveJavaObject(objects[0]);
                        ((Buffer)aggregationBuffer).value += " " + val;
                }

                public Object terminatePartial(AggregationBuffer aggregationBuffer) throws HiveException {
                        return ((Buffer)aggregationBuffer).value;
                }

                public void merge(AggregationBuffer aggregationBuffer, Object o) throws HiveException {
                        if(o != null) {
                                ((Buffer) aggregationBuffer).value += " " + soi.getPrimitiveJavaObject(o);
                        }
                }

                public Object terminate(AggregationBuffer aggregationBuffer) throws HiveException {
                        return ((Buffer)aggregationBuffer).value;
                }
        }
}
