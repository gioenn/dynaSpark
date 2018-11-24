package core.src.main.scala.org.apache.spark.scheduler;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class GuardEvaluator implements IGuardEvaluator {

	private List<Integer> satisfiableGuards;

    @Override
    public List<Integer> evaluateGuards(Map<String, Object> knownValues) {
        satisfiableGuards = new ArrayList<>();

        extractValues(knownValues);

        evaluateActualGuards();

        return satisfiableGuards;
    }
    
    private void evaluateActualGuards() {
        //path condition evaluation
    }
    
    private void extractValues(Map<String, Object> knownValues) {
    	//extract valid values
    }
}
