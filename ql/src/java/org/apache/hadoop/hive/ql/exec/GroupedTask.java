package org.apache.hadoop.hive.ql.exec;

import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.plan.api.StageType;

import java.io.Serializable;
import java.util.List;

public class GroupedTask<T extends Serializable> extends Task<T> {

  List<Task<?>> tasks;

  @Override
  protected int execute(DriverContext driverContext) {
    return 0;
  }

  @Override
  public StageType getType() {
    return null;
  }

  @Override
  public String getName() {
    return null;
  }
}
