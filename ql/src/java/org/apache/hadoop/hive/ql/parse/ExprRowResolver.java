package org.apache.hadoop.hive.ql.parse;

import org.apache.hadoop.hive.ql.exec.ColumnInfo;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ExprRowResolver extends RowResolver {

  private Map<String, ASTNode> expressionMap;
  private Map<String, ColumnInfo> erslvMap;

  public ExprRowResolver() {
    expressionMap = new HashMap<String, ASTNode>();
    erslvMap = new LinkedHashMap<String, ColumnInfo>();
  }

  /**
   * Puts a resolver entry corresponding to a source expression which is to be
   * used for identical expression recognition (e.g. for matching expressions
   * in the SELECT list with the GROUP BY clause).  The convention for such
   * entries is an empty-string ("") as the table alias together with the
   * string rendering of the ASTNode as the column alias.
   */
  public void putExpression(ASTNode node, ColumnInfo colInfo) {
    String treeAsString = node.toStringTree();
    expressionMap.put(treeAsString, node);
    erslvMap.put(treeAsString, colInfo);
    put(colInfo.getTabAlias(), colInfo.getInternalName(), colInfo);
  }

  /**
   * Retrieves the ColumnInfo corresponding to a source expression which
   * exactly matches the string rendering of the given ASTNode.
   */
  public ColumnInfo getExpression(ASTNode node) throws SemanticException {
    ColumnInfo colInfo = erslvMap.get(node.toStringTree());
    if (colInfo == null) {
      if (node.getType() == HiveParser.TOK_TABLE_OR_COL) {
        return getExpression(BaseSemanticAnalyzer.unescapeIdentifier(node
            .getChild(0).getText()));
      }
      if (node.getType() == HiveParser.DOT) {
        return erslvMap.get("(TOK_TABLE_OR_COL " +
            BaseSemanticAnalyzer.unescapeIdentifier(node.getChild(1).getText()) + ")");
      }
    }
    return colInfo;
  }

  private ColumnInfo getExpression(String column) throws SemanticException {
    ColumnInfo ret = null;
    boolean found = false;
    Pattern pattern = Pattern.compile("\\(\\. \\(TOK_TABLE_OR_COL (.+)\\) " + column +"\\)");
    for (Map.Entry<String, ColumnInfo> entry : erslvMap.entrySet()) {
      Matcher matcher = pattern.matcher(entry.getKey());
      if (matcher.matches() && matcher.group(1).equals(entry.getValue().getTabAlias())) {
        if (found) {
          throw new SemanticException("Column " + column
              + " Found in more than One Tables/Subqueries");
        }
        ret = entry.getValue();
        found = true;
      }
    }
    return ret;
  }

  /**
   * Retrieves the source expression matching a given ASTNode's
   * string rendering exactly.
   */
  public ASTNode getExpressionSource(ASTNode node) {
    return expressionMap.get(node.toStringTree());
  }

  @Override
  public boolean getIsExprResolver() {
    return true;
  }
}
