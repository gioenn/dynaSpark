package core.src.main.scala.org.apache.spark.scheduler;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class GuardEvaluator implements IGuardEvaluator {

    @Override
    public List<Integer> evaluateGuards(Map<String, Object> knownValues) {
 
        return new ArrayList<>();
    }	
}
