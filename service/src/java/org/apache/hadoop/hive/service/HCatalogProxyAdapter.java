package org.apache.hadoop.hive.service;

import com.facebook.fb303.FacebookBase;
import com.facebook.fb303.fb_status;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.thrift.TException;

import java.util.List;
import java.util.Map;

/**
 * User: thiruvel
 * Date: 6/7/12
 * Time: 3:09 PM
 *
 * This is a Proxy Layer which implements the MetaStore interface. Instead of executing the MetaStore API's,
 * this will take each API's arguments and proxy it to HCatalog. This may or may not be the best solution, but
 * this works reasonably well.
 *
 */
public class HCatalogProxyAdapter extends FacebookBase
    implements ThriftHiveMetastore.Iface {

  public static final Log LOG = LogFactory.getLog(HCatalogProxyAdapter.class.getName());
  private final HiveConf hiveConf;

  // TODO: Make sense to use conf for all metastore creation?
  protected HCatalogProxyAdapter(String name, HiveConf conf) {
    super(name);
    this.hiveConf = conf;
    LOG.info("Initializing HCatalog proxy layer");
  }

  // TODO: Is this really the right way? Are we safe when the same thread is running for > 7 days say?
  private final ThreadLocal<HiveMetaStoreClient> client =
    new ThreadLocal<HiveMetaStoreClient>() {
      @Override
      protected synchronized HiveMetaStoreClient initialValue() {
        try {
          return new HiveMetaStoreClient(hiveConf);
        } catch (MetaException e) {
          LOG.error("Cannot create HiveMetaStoreClient " + e.getMessage());
          throw new RuntimeException("Cannot create hive metastore client to talk to HCatalog", e);
        }
      }
    };


  private ThriftHiveMetastore.Iface getHCatClient() throws MetaException {
    HiveMetaStoreClient metaStoreClient = client.get();
    if (metaStoreClient == null) {
      LOG.error("Unable to create HiveMetaStoreClient, bailing out..");
      throw new MetaException("metastore client is null, cannot create one");
    }
    return metaStoreClient.getLowlevelClient();
  }

  // TODO: Is there a better way of dealing with this? Override shutdown?
  public void cleanup() {
    HiveMetaStoreClient metaStoreClient = client.get();
    if (metaStoreClient != null) {
      LOG.info("Cleaning up metaStore client for this connection");
      metaStoreClient.close();
      client.remove();
    }
  }

  @Override
  public String getVersion() throws TException {
    LOG.debug("Calling getVersion()");
    try {
      return getHCatClient().getVersion();
    } catch (MetaException e) {
      LOG.info("getVersion seems to have failed " + e.getMessage());
      throw new RuntimeException("getVersion() failed ", e);
    }
  }

  @Override
  public fb_status getStatus() {
    LOG.debug("Calling getStatus()");
    try {
      return getHCatClient().getStatus();
    } catch (TException e) {
      LOG.error("getStatus failed with " + e.getMessage());
      throw new RuntimeException("getStatus failed", e);
    } catch (MetaException e) {
      LOG.error("getStatus failed with " + e.getMessage());
      throw new RuntimeException("getStatus failed", e);
    }
  }

  @Override
  public String getCpuProfile(int i) throws TException {
    LOG.debug("Calling getCpuProfile() i=" + i);
    try {
      return getHCatClient().getCpuProfile(i);
    } catch (MetaException e) {
      LOG.error("getCpuProfile failed with " + e.getMessage());
      throw new RuntimeException("getCpuProfile failed", e);

    }
  }

  @Override
  public void create_database(Database database)
      throws AlreadyExistsException, InvalidObjectException, MetaException, TException {
    LOG.debug("create_database with database=" + database);
    getHCatClient().create_database(database);
  }

  @Override
  public Database get_database(String name)
      throws NoSuchObjectException, MetaException, TException {

    LOG.debug("get_database called with name=" + name);
    return getHCatClient().get_database(name);
  }

  @Override
  public void drop_database(String name, boolean deleteData, boolean cascade)
      throws NoSuchObjectException, InvalidOperationException, MetaException, TException {

    LOG.debug("drop_database name=" + name + " deleteData=" + deleteData + " cascade=" + cascade);
    getHCatClient().drop_database(name, deleteData, cascade);
  }

  @Override
  public List<String> get_databases(String pattern)
      throws MetaException, TException {

    LOG.debug("get_databases pattern=" + pattern);
    return getHCatClient().get_databases(pattern);
  }

  @Override
  public List<String> get_all_databases()
      throws MetaException, TException {

    LOG.debug("get_all_databases()");
    return getHCatClient().get_all_databases();
  }

  @Override
  public void alter_database(String dbname, Database db)
      throws MetaException, NoSuchObjectException, TException {

    LOG.debug("alter_database dbname=" + dbname + " db=" + db);
    getHCatClient().alter_database(dbname, db);
  }

  @Override
  public Type get_type(String name)
      throws MetaException, NoSuchObjectException, TException {

    LOG.debug("get_type() name=" + name);
    return getHCatClient().get_type(name);
  }

  @Override
  public boolean create_type(Type type)
      throws AlreadyExistsException, InvalidObjectException, MetaException, TException {

    LOG.debug("create_type type=" + type);
    return getHCatClient().create_type(type);
  }

  @Override
  public boolean drop_type(String type)
      throws MetaException, NoSuchObjectException, TException {

    LOG.debug("drop_type type=" + type);
    return getHCatClient().drop_type(type);
  }

  @Override
  public Map<String, Type> get_type_all(String name)
      throws MetaException, TException {

    LOG.debug("get_type_all name=" + name);
    return getHCatClient().get_type_all(name);
  }

  @Override
  public List<FieldSchema> get_fields(String db_name, String table_name)
      throws MetaException, UnknownTableException, UnknownDBException, TException {

    LOG.debug("get_fields db_name=" + db_name + " table_name=" + table_name);
    return getHCatClient().get_fields(db_name, table_name);
  }

  @Override
  public List<FieldSchema> get_schema(String db_name, String table_name)
      throws MetaException, UnknownTableException, UnknownDBException, TException {

    LOG.debug("get_schema db_name=" + db_name + " table_name=" + table_name);
    return getHCatClient().get_schema(db_name, table_name);
  }

  @Override
  public void create_table(Table tbl)
      throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException, TException {

    LOG.debug("create_table tbl=" + tbl);
    getHCatClient().create_table(tbl);
  }

/*
  @Override
  public void create_table_with_environment_context(Table tbl, EnvironmentContext environment_context)
      throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException, TException {

    LOG.debug("create_table_with_environment_context tbl=" + tbl + " env=" + environment_context);
    getHCatClient().create_table_with_environment_context(tbl, environment_context);
  }
  */

  @Override
  public void drop_table(String dbname, String name, boolean deleteData)
      throws NoSuchObjectException, MetaException, TException {

    LOG.debug("drop_type() dbname=" + dbname + " name=" + name + " deleteData=" + deleteData);
    getHCatClient().drop_table(dbname, name, deleteData);
  }

  @Override
  public List<String> get_tables(String db_name, String pattern)
      throws MetaException, TException {

    LOG.debug("Calling get_tables with db_name=" + db_name + " pattern=" + pattern);
    return getHCatClient().get_tables(db_name, pattern);
  }

  @Override
  public List<String> get_all_tables(String db_name)
      throws MetaException, TException {

    LOG.debug("Calling get_all_tables with db_name=" + db_name);
    return getHCatClient().get_all_tables(db_name);
  }

  @Override
  public Table get_table(String dbname, String tbl_name)
      throws MetaException, NoSuchObjectException, TException {

    LOG.debug("Calling get_table with args dbname=" + dbname + " tbl_name=" + tbl_name);
    return getHCatClient().get_table(dbname, tbl_name);
  }

  @Override
  public List<Table> get_table_objects_by_name(String dbname, List<String> tbl_names)
      throws MetaException, InvalidOperationException, UnknownDBException, TException {

    LOG.debug("get_table_objects_by_name() dbname=" + dbname + " tbl_names=" + tbl_names);
    return getHCatClient().get_table_objects_by_name(dbname, tbl_names);
  }

  @Override
  public List<String> get_table_names_by_filter(String dbname, String filter, short max_tables)
      throws MetaException, InvalidOperationException, UnknownDBException, TException {

    LOG.debug("get_table_names_by_filter() dbname=" + dbname + " filter=" + filter + " max_tables=" + max_tables);
    return getHCatClient().get_table_names_by_filter(dbname, filter, max_tables);
  }

  @Override
  public void alter_table(String dbname, String tbl_name, Table new_tbl)
      throws InvalidOperationException, MetaException, TException {

    LOG.debug("alter_table() dbname=" + dbname + " tbl_name=" + tbl_name + " new_tbl=" + new_tbl);
    getHCatClient().alter_table(dbname, tbl_name, new_tbl);
  }

/*
  @Override
  public void alter_table_with_environment_context(String dbname, String tbl_name, Table new_tbl, EnvironmentContext environment_context) throws InvalidOperationException, MetaException, TException {
    getHCatClient().alter_table_with_environment_context(dbname, tbl_name, new_tbl, environment_context);
  }
  */

  @Override
  public Partition add_partition(Partition new_part)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {

    LOG.debug("add_partition() new_part=" + new_part);
    return getHCatClient().add_partition(new_part);
  }

/*
  @Override
  public Partition add_partition_with_environment_context(Partition new_part, EnvironmentContext environment_context)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {

    LOG.debug("add_partition_with_environment_context() new_part=" + new_part + " env=" + environment_context);
    return getHCatClient().add_partition_with_environment_context(new_part, environment_context);
  }
  */

  @Override
  public int add_partitions(List<Partition> new_parts)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {

    LOG.debug("add_partitions() new_parts=" + new_parts);
    return getHCatClient().add_partitions(new_parts);
  }

  @Override
  public Partition append_partition(String db_name, String tbl_name, List<String> part_vals)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {

    LOG.debug("append_partition() dbname=" + db_name + " tbl_name=" + tbl_name + " part_vals=" + part_vals);
    return getHCatClient().append_partition(db_name, tbl_name, part_vals);
  }

  @Override
  public Partition append_partition_by_name(String db_name, String tbl_name, String part_name)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {

    LOG.debug("append_partition_by_name() db_name=" + db_name + " tbl_name=" + tbl_name + " part_name=" + part_name);
    return getHCatClient().append_partition_by_name(db_name, tbl_name, part_name);
  }

  @Override
  public boolean drop_partition(String db_name, String tbl_name, List<String> part_vals, boolean deleteData)
      throws NoSuchObjectException, MetaException, TException {

    LOG.debug("drop_partition() db=" + db_name + " tbl_name=" + tbl_name + " part_vals=" +
        part_vals + " delete=" + deleteData);
    return getHCatClient().drop_partition(db_name, tbl_name, part_vals, deleteData);
  }

  @Override
  public boolean drop_partition_by_name(String db_name, String tbl_name, String part_name, boolean deleteData)
      throws NoSuchObjectException, MetaException, TException {

    LOG.debug("drop_partition_by_name() db=" + db_name + " tbl_name=" + tbl_name +
        " part=" + part_name + " delete=" + deleteData);
    return getHCatClient().drop_partition_by_name(db_name, tbl_name, part_name, deleteData);
  }

  @Override
  public Partition get_partition(String db_name, String tbl_name, List<String> part_vals)
      throws MetaException, NoSuchObjectException, TException {

    LOG.debug("get_partition() db=" + db_name + " tbl=" + tbl_name + " part=" + part_vals);
    return getHCatClient().get_partition(db_name, tbl_name, part_vals);
  }

  @Override
  public Partition get_partition_with_auth(String db_name, String tbl_name, List<String> part_vals,
                                           String user_name, List<String> group_names)
      throws MetaException, NoSuchObjectException, TException {

    LOG.debug("get_partition_with_auth()");
    return getHCatClient().get_partition_with_auth(db_name, tbl_name, part_vals, user_name, group_names);
  }

  @Override
  public Partition get_partition_by_name(String db_name, String tbl_name, String part_name)
      throws MetaException, NoSuchObjectException, TException {

    LOG.debug("get_partition_by_name() db=" + db_name + " tbl_name=" + tbl_name + " part=" + part_name);
    return getHCatClient().get_partition_by_name(db_name, tbl_name, part_name);
  }

  @Override
  public List<Partition> get_partitions(String db_name, String tbl_name, short max_parts)
      throws NoSuchObjectException, MetaException, TException {

    LOG.debug("get_partitions() db=" + db_name + " tbl=" + tbl_name + " max=" + max_parts);
    return getHCatClient().get_partitions(db_name, tbl_name, max_parts);
  }

  @Override
  public List<Partition> get_partitions_with_auth(String db_name, String tbl_name, short max_parts,
                                                  String user_name, List<String> group_names)
      throws NoSuchObjectException, MetaException, TException {

    LOG.debug("get_partitions_with_auth()");
    return getHCatClient().get_partitions_with_auth(db_name, tbl_name, max_parts, user_name, group_names);
  }

  @Override
  public List<String> get_partition_names(String db_name, String tbl_name, short max_parts)
      throws MetaException, TException {

    LOG.debug("get_partition_names()");
    return getHCatClient().get_partition_names(db_name, tbl_name, max_parts);
  }

  @Override
  public List<Partition> get_partitions_ps(String db_name, String tbl_name, List<String> part_vals,
                                           short max_parts)
      throws MetaException, NoSuchObjectException, TException {

    LOG.debug("get_partitions_ps()");
    return getHCatClient().get_partitions_ps(db_name, tbl_name, part_vals, max_parts);
  }

  @Override
  public List<Partition> get_partitions_ps_with_auth(String db_name, String tbl_name,
                                                     List<String> part_vals, short max_parts,
                                                     String user_name, List<String> group_names)
      throws NoSuchObjectException, MetaException, TException {

    LOG.debug("get_partitions_ps_with_auth()");
    return getHCatClient().get_partitions_ps_with_auth(db_name, tbl_name, part_vals, max_parts, user_name, group_names);
  }

  @Override
  public List<String> get_partition_names_ps(String db_name, String tbl_name, List<String> part_vals,
                                             short max_parts)
      throws MetaException, NoSuchObjectException, TException {

    LOG.debug("get_partition_names_ps()");
    return getHCatClient().get_partition_names_ps(db_name, tbl_name, part_vals, max_parts);
  }

  @Override
  public List<Partition> get_partitions_by_filter(String db_name, String tbl_name, String filter,
                                                  short max_parts)
      throws MetaException, NoSuchObjectException, TException {

    LOG.debug("get_partitions_by_filter()");
    return getHCatClient().get_partitions_by_filter(db_name, tbl_name, filter, max_parts);
  }

  @Override
  public List<Partition> get_partitions_by_names(String db_name, String tbl_name, List<String> names)
      throws MetaException, NoSuchObjectException, TException {

    LOG.debug("get_partitions_by_names()");
    return getHCatClient().get_partitions_by_names(db_name, tbl_name, names);
  }

  @Override
  public void alter_partition(String db_name, String tbl_name, Partition new_part)
      throws InvalidOperationException, MetaException, TException {

    LOG.debug("alter_partition()");
    getHCatClient().alter_partition(db_name, tbl_name, new_part);
  }

/*
  @Override
  public void alter_partition_with_environment_context(String db_name, String tbl_name,
                                                       Partition new_part, EnvironmentContext environment_context)
      throws InvalidOperationException, MetaException, TException {

    LOG.debug("alter_partition_with_environment_context()");
    getHCatClient().alter_partition_with_environment_context(db_name, tbl_name, new_part, environment_context);
  }
  */

  @Override
  public void rename_partition(String db_name, String tbl_name, List<String> part_vals, Partition new_part)
      throws InvalidOperationException, MetaException, TException {

    LOG.debug("rename_partition()");
    getHCatClient().rename_partition(db_name, tbl_name, part_vals, new_part);
  }

  @Override
  public String get_config_value(String name, String defaultValue) throws ConfigValSecurityException, TException {
    LOG.debug("get_config_value name=" + name + " default=" + defaultValue);
    try {
      return getHCatClient().get_config_value(name, defaultValue);
    } catch (MetaException e) {
      LOG.error("get_config_value name=" + name + " default=" + defaultValue + " failed" + e.getMessage());
      throw new RuntimeException("get_config_value name=" + name + " default=" + defaultValue + " failed", e);
    }
  }

  @Override
  public List<String> partition_name_to_vals(String part_name) throws MetaException, TException {
    LOG.debug("partition_name_to_vals()");
    return getHCatClient().partition_name_to_vals(part_name);
  }

  @Override
  public Map<String, String> partition_name_to_spec(String part_name)
      throws MetaException, TException {

    LOG.debug("partition_name_to_spec()");
    return getHCatClient().partition_name_to_spec(part_name);
  }

  @Override
  public void markPartitionForEvent(String db_name, String tbl_name, Map<String, String> part_vals,
                                    PartitionEventType eventType)
      throws MetaException, NoSuchObjectException, UnknownDBException, UnknownTableException,
      UnknownPartitionException, InvalidPartitionException, TException {

    LOG.debug("markPartitionForEvent()");
    getHCatClient().markPartitionForEvent(db_name, tbl_name, part_vals, eventType);
  }

  @Override
  public boolean isPartitionMarkedForEvent(String db_name, String tbl_name,
                                           Map<String, String> part_vals, PartitionEventType eventType)
      throws MetaException, NoSuchObjectException, UnknownDBException, UnknownTableException,
      UnknownPartitionException, InvalidPartitionException, TException {

    LOG.debug("isPartitionMarkedForEvent()");
    return getHCatClient().isPartitionMarkedForEvent(db_name, tbl_name, part_vals, eventType);
  }

  @Override
  public Index add_index(Index new_index, Table index_table)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {

    LOG.debug("add_index()");
    return getHCatClient().add_index(new_index, index_table);
  }

  @Override
  public void alter_index(String dbname, String base_tbl_name, String idx_name, Index new_idx)
      throws InvalidOperationException, MetaException, TException {

    LOG.debug("alter_index()");
    getHCatClient().alter_index(dbname, base_tbl_name, idx_name, new_idx);
  }

  @Override
  public boolean drop_index_by_name(String db_name, String tbl_name, String index_name, boolean deleteData)
      throws NoSuchObjectException, MetaException, TException {

    LOG.debug("drop_index_by_name()");
    return getHCatClient().drop_index_by_name(db_name, tbl_name, index_name, deleteData);
  }

  @Override
  public Index get_index_by_name(String db_name, String tbl_name, String index_name)
      throws MetaException, NoSuchObjectException, TException {

    LOG.debug("get_index_by_name()");
    return getHCatClient().get_index_by_name(db_name, tbl_name, index_name);
  }

  @Override
  public List<Index> get_indexes(String db_name, String tbl_name, short max_indexes)
      throws NoSuchObjectException, MetaException, TException {

    LOG.debug("get_indexes()");
    return getHCatClient().get_indexes(db_name, tbl_name, max_indexes);
  }

  @Override
  public List<String> get_index_names(String db_name, String tbl_name, short max_indexes)
      throws MetaException, TException {

    LOG.debug("get_index_names()");
    return getHCatClient().get_index_names(db_name, tbl_name, max_indexes);
  }

  @Override
  public boolean create_role(Role role)
      throws MetaException, TException {

    LOG.debug("create_role()");
    return getHCatClient().create_role(role);
  }

  @Override
  public boolean drop_role(String role_name)
      throws MetaException, TException {

    LOG.debug("drop_role()");
    return getHCatClient().drop_role(role_name);
  }

  @Override
  public List<String> get_role_names() throws MetaException, TException {

    LOG.debug("get_role_names()");
    return getHCatClient().get_role_names();
  }

  @Override
  public boolean grant_role(String role_name, String principal_name, PrincipalType principal_type,
                            String grantor, PrincipalType grantorType, boolean grant_option)
      throws MetaException, TException {

    LOG.debug("grant_role()");
    return getHCatClient().grant_role(role_name, principal_name, principal_type, grantor, grantorType, grant_option);
  }

  @Override
  public boolean revoke_role(String role_name, String principal_name, PrincipalType principal_type)
      throws MetaException, TException {

    LOG.debug("revoke_role()");
    return getHCatClient().revoke_role(role_name, principal_name, principal_type);
  }

  @Override
  public List<Role> list_roles(String principal_name, PrincipalType principal_type)
      throws MetaException, TException {

    LOG.debug("list_roles()");
    return getHCatClient().list_roles(principal_name, principal_type);
  }

  @Override
  public PrincipalPrivilegeSet get_privilege_set(HiveObjectRef hiveObject, String user_name,
                                                 List<String> group_names)
      throws MetaException, TException {

    LOG.debug("get_privilege_set()");
    return getHCatClient().get_privilege_set(hiveObject, user_name, group_names);
  }

  @Override
  public List<HiveObjectPrivilege> list_privileges(String principal_name,
                                                   PrincipalType principal_type, HiveObjectRef hiveObject)
      throws MetaException, TException {

    LOG.debug("list_privileges()");
    return getHCatClient().list_privileges(principal_name, principal_type, hiveObject);
  }

  @Override
  public boolean grant_privileges(PrivilegeBag privileges) throws MetaException, TException {

    LOG.debug("grant_privileges()");
    return getHCatClient().grant_privileges(privileges);
  }

  @Override
  public boolean revoke_privileges(PrivilegeBag privileges) throws MetaException, TException {

    LOG.debug("revoke_privileges()");
    return getHCatClient().revoke_privileges(privileges);
  }

  /* TODO: Just redirecting to HCatalog is really correct? */
  @Override
  public List<String> set_ugi(String user_name, List<String> group_names)
      throws MetaException, TException {

    LOG.debug("set_ugi()");
    return getHCatClient().set_ugi(user_name, group_names);
  }

  @Override
  public String get_delegation_token(String token_owner, String renewer_kerberos_principal_name)
      throws MetaException, TException {

    LOG.debug("get_delegation_token()");
    return getHCatClient().get_delegation_token(token_owner, renewer_kerberos_principal_name);
  }

  @Override
  public long renew_delegation_token(String token_str_form) throws MetaException, TException {

    LOG.debug("renew_delegation_token()");
    return getHCatClient().renew_delegation_token(token_str_form);
  }

  @Override
  public void cancel_delegation_token(String token_str_form) throws MetaException, TException {

    LOG.debug("cancel_delegation_token()");
    getHCatClient().cancel_delegation_token(token_str_form);
  }
}
