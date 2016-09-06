package pl.isa.hadoop;


import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

@Description(
        value = "__FUNC__ returns hello + input"
)
public class HelloWorldUdf extends UDF {
        public String evaluate(String input) {
                if (input == null) {
                        return null;
                }

                return "hello " + input;
        }
}
