package org.apache.hadoop.hive.ql.metadata;

import org.antlr.runtime.TokenRewriteStream;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.QBParseInfo;

public interface HiveStorageSubQueryHandler {

  TableScanOperator handleSubQuery(
      QBParseInfo parseInfo,
      org.apache.hadoop.hive.metastore.api.Table table,
      TokenRewriteStream rewriter, ASTNode source);
}
