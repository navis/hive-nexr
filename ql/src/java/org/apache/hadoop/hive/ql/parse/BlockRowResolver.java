package org.apache.hadoop.hive.ql.parse;

import org.apache.hadoop.hive.ql.exec.ColumnInfo;

public class BlockRowResolver extends RowResolver {
  
  private RowResolver output;

  public BlockRowResolver(RowResolver input, RowResolver output) {
    input.copy(this);
    this.output = output;
  }

  @Override
  public ColumnInfo get(String tab_alias, String col_alias) throws SemanticException {
    ColumnInfo resolved = output.get(tab_alias, col_alias);
    if (resolved != null) {
      return new ColumnInfo(resolved.getInternalName(), resolved.getType(),
          resolved.getTabAlias(), resolved.isHiddenVirtualCol());
    }
    ColumnInfo column = super.get(tab_alias, col_alias);
    if (column != null) {
      return column;
    }
    return null;
  }

  @Override
  public String[] reverseLookup(String internalName) {
    String[] reverse = super.reverseLookup(internalName);
    if (reverse == null) {
      reverse = output.reverseLookup(internalName);
    }
    return reverse;
  }
}
