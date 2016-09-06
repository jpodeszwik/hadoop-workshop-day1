package pl.isa.hadoop;

import com.google.common.collect.Maps;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.Map;

public class CountsUdaf extends AbstractGenericUDAFResolver {
        public GenericUDAFEvaluator getEvaluator(TypeInfo[] info) throws SemanticException {
                return new Evaluator();
        }

        public static class Evaluator extends GenericUDAFEvaluator{
                private ObjectInspector keyObjectInspector;
                private ObjectInspector valueObjectInspector;

                private StandardMapObjectInspector moi;
                public static class Buffer implements AggregationBuffer {
                        Map<String, Integer> counts = Maps.newHashMap();
                }

                public ObjectInspector init(GenericUDAFEvaluator.Mode m, ObjectInspector[] parameters) throws HiveException {
                        super.init(m, parameters);

                        if(parameters[0] instanceof StandardMapObjectInspector) {
                                moi = (StandardMapObjectInspector) parameters[0];
                                keyObjectInspector = moi.getMapKeyObjectInspector();
                                valueObjectInspector = moi.getMapValueObjectInspector();
                        } else {
                                keyObjectInspector = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
                                valueObjectInspector = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
                        }

                        return ObjectInspectorFactory.getStandardMapObjectInspector(
                                PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                                PrimitiveObjectInspectorFactory.javaIntObjectInspector
                        );
                }

                public AggregationBuffer getNewAggregationBuffer() throws HiveException {
                        return new Buffer();
                }

                public void reset(AggregationBuffer aggregationBuffer) throws HiveException {
                        ((Buffer)aggregationBuffer).counts = Maps.newHashMap();
                }

                public void iterate(AggregationBuffer aggregationBuffer, Object[] objects) throws HiveException {
                        increment((Buffer)aggregationBuffer, objects[0], 1);
                }

                public Object terminatePartial(AggregationBuffer aggregationBuffer) throws HiveException {
                        return ((Buffer)aggregationBuffer).counts;
                }

                public void merge(AggregationBuffer aggregationBuffer, Object o) throws HiveException {
                        if(o != null) {


                                Map<Object, Object> rawMap = (Map<Object, Object>) moi.getMap(o);
                                for(Map.Entry<Object, Object> entry : rawMap.entrySet()) {
                                        increment((Buffer)aggregationBuffer, entry.getKey(), entry.getValue());
                                }
                        }
                }

                public Object terminate(AggregationBuffer aggregationBuffer) throws HiveException {
                        return ((Buffer)aggregationBuffer).counts;
                }

                private void increment(Buffer buffer, Object obj, Object value) {
                        int intValue = (Integer)ObjectInspectorUtils.copyToStandardJavaObject(value, this.valueObjectInspector);
                        int last = 0;
                        String method = obj.toString();
                        if(buffer.counts.containsKey(method)) {
                                last = buffer.counts.get(method);
                        }
                        buffer.counts.put(method, last + intValue);
                }
        }
}
