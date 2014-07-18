/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.security.authorization;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.HiveObjectType;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;
import org.apache.hadoop.hive.ql.metadata.AuthorizationException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;

public abstract class BitSetCheckedAuthorizationProvider extends
    HiveAuthorizationProviderBase {

  static class BitSetChecker {

    boolean[] inputCheck = null;
    boolean[] outputCheck = null;

    public static BitSetChecker getBitSetChecker(Privilege[] inputRequiredPriv,
        Privilege[] outputRequiredPriv) {
      BitSetChecker checker = new BitSetChecker();
      if (inputRequiredPriv != null) {
        checker.inputCheck = new boolean[inputRequiredPriv.length];
        for (int i = 0; i < checker.inputCheck.length; i++) {
          checker.inputCheck[i] = false;
        }
      }
      if (outputRequiredPriv != null) {
        checker.outputCheck = new boolean[outputRequiredPriv.length];
        for (int i = 0; i < checker.outputCheck.length; i++) {
          checker.outputCheck[i] = false;
        }
      }

      return checker;
    }

  }

  @Override
  public void authorize(Privilege[] inputRequiredPriv,
      Privilege[] outputRequiredPriv, boolean grantedOnly)
      throws HiveException, AuthorizationException {

    BitSetChecker checker = BitSetChecker.getBitSetChecker(inputRequiredPriv,
        outputRequiredPriv);
    boolean[] inputCheck = checker.inputCheck;
    boolean[] outputCheck = checker.outputCheck;

    authorizeUserPriv(inputRequiredPriv, inputCheck, outputRequiredPriv,
        outputCheck, grantedOnly);
    checkAndThrowAuthorizationException(inputRequiredPriv, outputRequiredPriv,
        inputCheck, outputCheck, null, null, null, null);
  }

  @Override
  public void authorize(Database db, Privilege[] inputRequiredPriv,
      Privilege[] outputRequiredPriv, boolean grantedOnly)
      throws HiveException, AuthorizationException {

    BitSetChecker checker = BitSetChecker.getBitSetChecker(inputRequiredPriv,
        outputRequiredPriv);
    boolean[] inputCheck = checker.inputCheck;
    boolean[] outputCheck = checker.outputCheck;

    authorizeUserAndDBPriv(db, inputRequiredPriv, outputRequiredPriv,
        inputCheck, outputCheck, grantedOnly);

    checkAndThrowAuthorizationException(inputRequiredPriv, outputRequiredPriv,
        inputCheck, outputCheck, db.getName(), null, null, null);
  }

  @Override
  public void authorize(Table table, Privilege[] inputRequiredPriv,
      Privilege[] outputRequiredPriv, boolean grantedOnly) throws HiveException {

    BitSetChecker checker = BitSetChecker.getBitSetChecker(inputRequiredPriv,
        outputRequiredPriv);
    boolean[] inputCheck = checker.inputCheck;
    boolean[] outputCheck = checker.outputCheck;

    authorizeUserDBAndTable(table, inputRequiredPriv,
        outputRequiredPriv, inputCheck, outputCheck, grantedOnly);

    checkAndThrowAuthorizationException(inputRequiredPriv, outputRequiredPriv,
        inputCheck, outputCheck, table.getDbName(), table.getTableName(),
        null, null);
  }

  @Override
  public void authorize(Partition part, Privilege[] inputRequiredPriv,
      Privilege[] outputRequiredPriv, boolean grantedOnly) throws HiveException {

    //if the partition does not have partition level privilege, go to table level.
    Table table = part.getTable();
    if (table.getParameters().get("PARTITION_LEVEL_PRIVILEGE") == null || ("FALSE"
        .equalsIgnoreCase(table.getParameters().get(
            "PARTITION_LEVEL_PRIVILEGE")))) {
      authorize(part.getTable(), inputRequiredPriv, outputRequiredPriv, grantedOnly);
      return;
    }

    BitSetChecker checker = BitSetChecker.getBitSetChecker(inputRequiredPriv,
        outputRequiredPriv);
    boolean[] inputCheck = checker.inputCheck;
    boolean[] outputCheck = checker.outputCheck;

    if (authorizeUserDbAndPartition(part, inputRequiredPriv, outputRequiredPriv,
        inputCheck, outputCheck, grantedOnly)) {
      return;
    }

    checkAndThrowAuthorizationException(inputRequiredPriv, outputRequiredPriv,
        inputCheck, outputCheck, part.getTable().getDbName(), part
            .getTable().getTableName(), part.getName(), null);
  }

  @Override
  public void authorize(Table table, Partition part, List<String> columns,
      Privilege[] inputRequiredPriv, Privilege[] outputRequiredPriv, boolean grantedOnly)
      throws HiveException {

    BitSetChecker checker = BitSetChecker.getBitSetChecker(inputRequiredPriv,
        outputRequiredPriv);
    boolean[] inputCheck = checker.inputCheck;
    boolean[] outputCheck = checker.outputCheck;

    String partName = null;
    List<String> partValues = null;
    if (part != null
        && (table.getParameters().get("PARTITION_LEVEL_PRIVILEGE") != null && ("TRUE"
            .equalsIgnoreCase(table.getParameters().get(
                "PARTITION_LEVEL_PRIVILEGE"))))) {
      partName = part.getName();
      partValues = part.getValues();
    }

    if (partValues == null) {
      if (authorizeUserDBAndTable(table, inputRequiredPriv, outputRequiredPriv,
          inputCheck, outputCheck, grantedOnly)) {
        return;
      }
    } else {
      if (authorizeUserDbAndPartition(part, inputRequiredPriv,
          outputRequiredPriv, inputCheck, outputCheck, grantedOnly)) {
        return;
      }
    }

    for (String col : columns) {

      BitSetChecker checker2 = BitSetChecker.getBitSetChecker(
          inputRequiredPriv, outputRequiredPriv);
      boolean[] inputCheck2 = checker2.inputCheck;
      boolean[] outputCheck2 = checker2.outputCheck;

      List<String> partColumnPrivileges = hive_db
          .get_privilege_set(HiveObjectType.COLUMN, table.getDbName(), table.getTableName(),
              partValues, col, this.getAuthenticator().getUserName(), this
                  .getAuthenticator().getGroupNames(), grantedOnly);

      authorizePrivileges(partColumnPrivileges, inputRequiredPriv, inputCheck2,
          outputRequiredPriv, outputCheck2);

      if (inputCheck2 != null) {
        booleanArrayOr(inputCheck2, inputCheck);
      }
      if (outputCheck2 != null) {
        booleanArrayOr(inputCheck2, inputCheck);
      }

      checkAndThrowAuthorizationException(inputRequiredPriv,
          outputRequiredPriv, inputCheck2, outputCheck2, table.getDbName(),
          table.getTableName(), partName, col);
    }
  }

  protected boolean authorizeUserPriv(Privilege[] inputRequiredPriv,
      boolean[] inputCheck, Privilege[] outputRequiredPriv,
      boolean[] outputCheck, boolean grantedOnly) throws HiveException {
    List<String> privileges = hive_db.get_privilege_set(
        HiveObjectType.GLOBAL, null, null, null, null, this.getAuthenticator()
            .getUserName(), this.getAuthenticator().getGroupNames(), grantedOnly);
    return authorizePrivileges(privileges, inputRequiredPriv, inputCheck,
        outputRequiredPriv, outputCheck);
  }

  /**
   * Check privileges on User and DB. This is used before doing a check on
   * table/partition objects, first check the user and DB privileges. If it
   * passed on this check, no need to check against the table/partition hive
   * object.
   *
   * @param db
   * @param inputRequiredPriv
   * @param outputRequiredPriv
   * @param inputCheck
   * @param outputCheck
   * @return true if the check on user and DB privilege passed, which means no
   *         need for privilege check on concrete hive objects.
   * @throws HiveException
   */
  private boolean authorizeUserAndDBPriv(Database db,
      Privilege[] inputRequiredPriv, Privilege[] outputRequiredPriv,
      boolean[] inputCheck, boolean[] outputCheck, boolean grantedOnly) throws HiveException {
    if (authorizeUserPriv(inputRequiredPriv, inputCheck, outputRequiredPriv,
        outputCheck, grantedOnly)) {
      return true;
    }


    List<String> dbPrivileges = hive_db.get_privilege_set(
        HiveObjectType.DATABASE, db.getName(), null, null, null, this
            .getAuthenticator().getUserName(), this.getAuthenticator()
            .getGroupNames(), grantedOnly);

    return authorizePrivileges(dbPrivileges, inputRequiredPriv, inputCheck,
        outputRequiredPriv, outputCheck);
  }

  /**
   * Check privileges on User, DB and table objects.
   *
   * @param table
   * @param inputRequiredPriv
   * @param outputRequiredPriv
   * @param inputCheck
   * @param outputCheck
   * @return true if the check passed
   * @throws HiveException
   */
  private boolean authorizeUserDBAndTable(Table table,
      Privilege[] inputRequiredPriv, Privilege[] outputRequiredPriv,
      boolean[] inputCheck, boolean[] outputCheck, boolean grantedOnly) throws HiveException {

    if (authorizeUserAndDBPriv(hive_db.getDatabase(table.getDbName()),
            inputRequiredPriv, outputRequiredPriv, inputCheck, outputCheck, grantedOnly)) {
      return true;
    }

    List<String> tablePrivileges = hive_db.get_privilege_set(
        HiveObjectType.TABLE, table.getDbName(), table.getTableName(), null,
        null, this.getAuthenticator().getUserName(), this.getAuthenticator()
            .getGroupNames(), grantedOnly);

    return authorizePrivileges(tablePrivileges, inputRequiredPriv, inputCheck,
        outputRequiredPriv, outputCheck);
  }

  /**
   * Check privileges on User, DB and table/Partition objects.
   *
   * @param part
   * @param inputRequiredPriv
   * @param outputRequiredPriv
   * @param inputCheck
   * @param outputCheck
   * @return true if the check passed
   * @throws HiveException
   */
  private boolean authorizeUserDbAndPartition(Partition part,
      Privilege[] inputRequiredPriv, Privilege[] outputRequiredPriv,
      boolean[] inputCheck, boolean[] outputCheck, boolean grantedOnly) throws HiveException {

    if (authorizeUserAndDBPriv(
        hive_db.getDatabase(part.getTable().getDbName()), inputRequiredPriv,
        outputRequiredPriv, inputCheck, outputCheck, grantedOnly)) {
      return true;
    }

    List<String> partPrivileges = part.getTPartition().getPrivileges();
    if (partPrivileges == null) {
      partPrivileges = hive_db.get_privilege_set(HiveObjectType.PARTITION, part
          .getTable().getDbName(), part.getTable().getTableName(), part
          .getValues(), null, this.getAuthenticator().getUserName(), this
          .getAuthenticator().getGroupNames(), grantedOnly);
    }

    return authorizePrivileges(partPrivileges, inputRequiredPriv, inputCheck,
        outputRequiredPriv, outputCheck);
  }

  protected boolean authorizePrivileges(List<String> privileges,
      Privilege[] inputPriv, boolean[] inputCheck, Privilege[] outputPriv,
      boolean[] outputCheck) throws HiveException {

    boolean pass = true;
    if (inputPriv != null) {
      pass = pass && matchPrivs(inputPriv, privileges, inputCheck);
    }
    if (outputPriv != null) {
      pass = pass && matchPrivs(outputPriv, privileges, outputCheck);
    }
    return pass;
  }

  /**
   * try to match an array of privileges from user/groups/roles grants.
   *
   * @param container
   */
  private boolean matchPrivs(Privilege[] required, List<String> privileges, boolean[] check) {

    if (required == null || required.length == 0) {
      return true;
    }

    if (privileges == null || privileges.isEmpty()) {
      return false;
    }

    for (String priv : privileges) {
      if (priv.equalsIgnoreCase(Privilege.ALL.toString())) {
        setBooleanArray(check, true);
        return true;
      }
    }

    for (int i = 0; i < required.length; i++) {
      String toMatch = required[i].toString();
      if (!check[i]) {
        check[i] = privileges.contains(toMatch.toLowerCase());
      }
    }

    return firstFalseIndex(check) < 0;
  }

  private List<String> getPrivilegeStringList(
      Collection<List<PrivilegeGrantInfo>> privCollection) {
    List<String> userPrivs = new ArrayList<String>();
    if (privCollection!= null && privCollection.size()>0) {
      for (List<PrivilegeGrantInfo> grantList : privCollection) {
        if (grantList == null){
          continue;
        }
        for (int i = 0; i < grantList.size(); i++) {
          PrivilegeGrantInfo grant = grantList.get(i);
          userPrivs.add(grant.getPrivilege());
        }
      }
    }
    return userPrivs;
  }

  private static void setBooleanArray(boolean[] check, boolean b) {
    for (int i = 0; i < check.length; i++) {
      check[i] = b;
    }
  }

  private static void booleanArrayOr(boolean[] output, boolean[] input) {
    for (int i = 0; i < output.length && i < input.length; i++) {
      output[i] = output[i] || input[i];
    }
  }

  private void checkAndThrowAuthorizationException(
      Privilege[] inputRequiredPriv, Privilege[] outputRequiredPriv,
      boolean[] inputCheck, boolean[] outputCheck,String dbName,
      String tableName, String partitionName, String columnName) {

    if (((inputCheck == null || firstFalseIndex(inputCheck) < 0)) &&
      (outputCheck == null || firstFalseIndex(outputCheck) < 0)) {
      return;
    }

    String userName = getAuthenticator().getUserName();

    String hiveObject = "{ ";
    if (dbName != null) {
      hiveObject = hiveObject + "database:" + dbName;
    }
    if (tableName != null) {
      hiveObject = hiveObject + ", table:" + tableName;
    }
    if (partitionName != null) {
      hiveObject = hiveObject + ", partitionName:" + partitionName;
    }
    if (columnName != null) {
      hiveObject = hiveObject + ", columnName:" + columnName;
    }
    if (hiveObject.length() == 2) {
      hiveObject += "global";
    }
    hiveObject = hiveObject + "}";

    if (inputCheck != null) {
      int input = this.firstFalseIndex(inputCheck);
      if (input >= 0) {
        throw new AuthorizationException(
          getMessage(inputRequiredPriv[input], hiveObject, userName));
      }
    }

    if (outputCheck != null) {
      int output = this.firstFalseIndex(outputCheck);
      if (output >= 0) {
        throw new AuthorizationException(
          getMessage(outputRequiredPriv[output], hiveObject, userName));
      }
    }
  }

  private String getMessage(Privilege priv, String object, String user) {
    return "No privilege '" + priv + "' found for outputs " + object +
      (user == null ? null : " from user " + user);
  }

  private int firstFalseIndex(boolean[] inputCheck) {
    return firstFalseIndex(inputCheck, 0);
  }

  private int firstFalseIndex(boolean[] inputCheck, int start) {
    if (inputCheck != null) {
      for (int i = start; i < inputCheck.length; i++) {
        if (!inputCheck[i]) {
          return i;
        }
      }
    }
    return -1;
  }
}
