package pl.isa.hadoop;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import java.util.ArrayList;
import java.util.List;

public class StructUdf extends GenericUDF {
        private StringObjectInspector oi1;
        private StringObjectInspector oi2;

        public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
                if(objectInspectors.length != 2) {
                        throw new UDFArgumentException("not enought arguments");
                }

                ObjectInspector oi1 = objectInspectors[0];
                if(oi1.getCategory() != ObjectInspector.Category.PRIMITIVE ||
                        ((PrimitiveObjectInspector)oi1).getPrimitiveCategory()
                                != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
                        throw new UDFArgumentException("wrong type of first param");
                }
                this.oi1 = (StringObjectInspector) oi1;

                ObjectInspector oi2 = objectInspectors[1];
                if(oi2.getCategory() != ObjectInspector.Category.PRIMITIVE ||
                        ((PrimitiveObjectInspector)oi2).getPrimitiveCategory()
                                != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
                        throw new UDFArgumentException("wrong type of second param");
                }
                this.oi2 = (StringObjectInspector) oi2;

                StringObjectInspector soi = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
                List<ObjectInspector> ois = new ArrayList<ObjectInspector>();
                ois.add(soi);
                ois.add(soi);

                List<String> fields = new ArrayList<String>();
                fields.add("first");
                fields.add("second");

                return ObjectInspectorFactory.getStandardStructObjectInspector(fields, ois);
        }

        public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
                String firstArgument = oi1.getPrimitiveJavaObject(deferredObjects[0].get());
                String secondArgument = oi2.getPrimitiveJavaObject(deferredObjects[1].get());

                return new Object[]{firstArgument, secondArgument};
        }

        public String getDisplayString(String[] strings) {
                return null;
        }
}
