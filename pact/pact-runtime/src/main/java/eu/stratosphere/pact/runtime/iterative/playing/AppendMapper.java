package eu.stratosphere.pact.runtime.iterative.playing;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;

@Deprecated
abstract class AppendMapper extends MapStub {

  @Override
  public void map(PactRecord record, Collector<PactRecord> out) throws Exception {
    String value = record.getField(0, PactString.class).getValue();
    value += appendix();
    record.setField(0, new PactString(value));
    out.collect(record);
  }

  abstract String appendix();

  public static class AppendHeadMapper extends AppendMapper {
    @Override
    String appendix() {
      return "-Head";
    }
  }

  public static class AppendIntermediateMapper extends AppendMapper {
    @Override
    String appendix() {
      return "-Intermediate";
    }
  }

  public static class AppendTailMapper extends AppendMapper {
    @Override
    String appendix() {
      return "-Tail";
    }
  }
}
