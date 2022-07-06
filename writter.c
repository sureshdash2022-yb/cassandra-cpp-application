#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include "cassandra.h"

//#define UNIVERSE_01 1
typedef struct ThreadMetaInfo {
  CassSession* session;
  int thread_id;
  char uuid[500];
  char table_name[100];
  int start;
  int end;
}ThreadMetaInfo;

void print_error(CassFuture* future) {
  const char* message;
  size_t message_length;
  cass_future_error_message(future, &message, &message_length);
  fprintf(stderr, "Error: %.*s\n", (int)message_length, message);
}

// Create a new cluster.
CassCluster* create_cluster(const char* hosts) {
  CassCluster* cluster = cass_cluster_new();
  cass_cluster_set_contact_points(cluster, hosts);
  return cluster;
}

// Connect to the cluster given a session.
CassError connect_session(CassSession* session, const CassCluster* cluster) {
  CassError rc = CASS_OK;
  CassFuture* future = cass_session_connect(session, cluster);

  cass_future_wait(future);
  rc = cass_future_error_code(future);
  if (rc != CASS_OK) {
    print_error(future);
  }
  cass_future_free(future);

  return rc;
}

CassError execute_query(CassSession* session, const char* query) {
  CassError rc = CASS_OK;
  CassFuture* future = NULL;
  CassStatement* statement = cass_statement_new(query, 0);

  future = cass_session_execute(session, statement);

  cass_future_wait(future);

  rc = cass_future_error_code(future);
  if (rc != CASS_OK) {
    print_error(future);
  }

  cass_future_free(future);
  cass_statement_free(statement);

  return rc;
}
#ifdef UNIVERSE_01
#define BUFFER_SIZE 500
#else
#define BUFFER_SIZE 5000
#endif

void *execute_and_log_select_thread(void* s) {
  ThreadMetaInfo* thread_info = (ThreadMetaInfo*)s;
  CassSession *session = thread_info->session;
  CassError rc = CASS_OK;
  CassFuture* future = NULL;
  int start_idx = thread_info->start;
  int cnt = 0;

  char query[BUFFER_SIZE] = {0};
  //printf("insert record for table: %s\n", thread_info->table_name);

  while (start_idx <= thread_info->end) {
  #ifdef UNIVERSE_01
  snprintf(query, BUFFER_SIZE, "INSERT INTO %s (k, v) VALUES('%s:k:%d','%s:v:%d')",
                       thread_info->table_name, thread_info->uuid,
                       start_idx,
                       thread_info->uuid,
                       start_idx);
  #else
  snprintf(query, BUFFER_SIZE, "INSERT INTO %s (hrtid, eventid, asof, effective, effective_end, sticky, hides_hrtid, validates_hrtid,hrt_symbol, updates_hrt_symbol, listed, updates_listed, listing_mic, updates_listing_mic, currency, updates_currency, isin, updates_isin,cusip, updates_cusip, sedol, updates_sedol, bb_melded_name, updates_bb_melded_name, bb_exch_symbol, updates_bb_exch_symbol,bb_composite_exch_code, updates_bb_composite_exch_code, bb_unique_id, updates_bb_unique_id, bb_region, updates_bb_region, figi, updates_figi,composite_figi, updates_composite_figi, share_class_figi, updates_share_class_figi, bb_global_company_id, updates_bb_global_company_id,recrunch_tag, bb_venue_figis_and_mics)  VALUES (%d, 'event-id-%d', %d, 1, 1, true, true, true, 'hrt_symbol', true, true, true, 'listing_mic',true, 'currency', true, 'isin', true, 'cusip', true, 'sedol', true, 'bb_melded_name', true, 'bb_exch_symbol', true, 'bb_composite_exch_code',true, 'bb_unique_id', true, 'bb_region', true, 'figi', true, 'composite_figi', true, 'composite_figi', true, 'bb_global_company_id-%d', true, 1, '{ \"name\": \"HRT\"}');", thread_info->table_name, start_idx, start_idx, start_idx, start_idx);
  #endif
  CassStatement* statement = cass_statement_new(query, 0);
  //cass_statement_set_consistency(statement, CASS_CONSISTENCY_QUORUM);

  future = cass_session_execute(session, statement);
  cass_future_wait(future);

  rc = cass_future_error_code(future);
  if (rc != CASS_OK) {
    print_error(future);
    if (rc == CASS_ERROR_LIB_NO_HOSTS_AVAILABLE) {
       break;
    }

  } else {
    start_idx += 1;
    cnt += 1;
    printf("[%d]:Total row are inserted by thread_id: %d\n", cnt, thread_info->thread_id);
  }
  cass_future_free(future);
  cass_statement_free(statement);
  }
}

CassError execute_and_log_select(CassSession* session) {
  CassError rc = CASS_OK;
  CassFuture* future = NULL;
  char query[500] = {0};

  snprintf(query, 500, "INSERT INTO %s (k, v) VALUES('6158934f-2675-4055-912d-3996f89fb0aa:k:%d',\
                       '6158934f-2675-4055-912d-3996f89fb0aa:v:%d')", "ybdemo_keyspace.test1", 1, 1);
  CassStatement* statement = cass_statement_new(query, 0);

  future = cass_session_execute(session, statement);
  cass_future_wait(future);

  rc = cass_future_error_code(future);
  if (rc != CASS_OK) {
    print_error(future);
  } else {
    printf("Row insert is successful.\n");
  }
  cass_future_free(future);
  cass_statement_free(statement);

  return rc;
}

int main(int argc, char** argv) {
  // Ensure you log errors.
  cass_log_set_level(CASS_LOG_ERROR);

  CassCluster* cluster = NULL;
  CassSession* session = cass_session_new();
  CassFuture* close_future = NULL;
  //char* hosts = "127.0.0.1";
  #ifdef UNIVERSE_01
  char* hosts = "10.150.5.51,10.150.5.56,10.150.5.64";
  #else
  //char* hosts = "10.150.1.41, 10.150.1.25, 10.150.1.31";
  char* hosts = "10.150.0.159,10.150.1.11,10.150.0.198";
  #endif
  if (argc <= 1) {
    fprintf(stderr, "provide <exe-name> <table-name> <uuid> <total-insert-records>\n");
    return -1;
  }

  cluster = create_cluster(hosts);
  cass_cluster_set_load_balance_dc_aware(cluster, "us-west1", 0, cass_false);

  if (connect_session(session, cluster) != CASS_OK) {
    cass_cluster_free(cluster);
    cass_session_free(session);
    return -1;
  }

  CassError rc = CASS_OK;
  execute_query(session, "CREATE KEYSPACE ybdemo_keyspace;");
  char query[BUFFER_SIZE] = {0};

  #ifdef UNIVERSE_01
    char* table_name = "test1";
    snprintf(query, BUFFER_SIZE, "CREATE TABLE %s (k TEXT PRIMARY KEY, v TEXT);", argv[1]);

  #else
  char* create_table_query = "CREATE TABLE %s ( "
    " hrtid bigint,"
    " eventid text,"
    " asof bigint,"
    " effective int,"
    " effective_end int,"
    " sticky boolean,"
    " hides_hrtid boolean,"
    " validates_hrtid boolean,"
    " hrt_symbol text,"
    " updates_hrt_symbol boolean,"
    " listed boolean,"
    " updates_listed boolean,"
    " listing_mic text,"
    " updates_listing_mic boolean,"
    " currency text,"
    " updates_currency boolean,"
    " isin text,"
    " updates_isin boolean,"
    " cusip text,"
    " updates_cusip boolean,"
    " sedol text,"
    " updates_sedol boolean,"
    " bb_melded_name text,"
    " updates_bb_melded_name boolean,"
    " bb_exch_symbol text,"
    " updates_bb_exch_symbol boolean,"
    " bb_composite_exch_code text,"
    " updates_bb_composite_exch_code boolean,"
    " bb_unique_id text,"
    " updates_bb_unique_id boolean,"
    " bb_region text, "
    " updates_bb_region boolean, "
    " figi text, "
    " updates_figi boolean, "
    " composite_figi text, "
    " updates_composite_figi boolean,"
    " share_class_figi text, "
    " updates_share_class_figi boolean,"
    " bb_global_company_id text, "
    " updates_bb_global_company_id boolean, "
    " recrunch_tag bigint, "
    " bb_venue_figis_and_mics jsonb, "
    " PRIMARY KEY (hrtid, eventid, asof));";

  snprintf(query, BUFFER_SIZE, create_table_query, argv[1]);
  execute_query(session, query);

/*
   create another table do insert paralley with it.
*/
#if 1
 char new_table[500];
 snprintf(new_table, 500, "%s_%d", argv[1], 1);
 snprintf(query, BUFFER_SIZE, create_table_query, new_table);
 execute_query(session, query);

  snprintf(query, 500, "TRUNCATE TABLE %s", new_table);
  execute_query(session, query);
#endif
#endif
  // Turncate table
  snprintf(query, 500, "TRUNCATE TABLE %s", argv[1]);
  execute_query(session, query);


  //rc = execute_and_log_select(session);
#define THREAD_CNT  50
  pthread_t thread_id[THREAD_CNT];
  ThreadMetaInfo* metainfo_list[THREAD_CNT];

  int idx = 0;
  printf("Before Thread\n");
  int total_insert_records = atoi(argv[3]);
  int range_count = total_insert_records/THREAD_CNT;
  int start_idx = 0;
  int end_idx = range_count;
  for (; idx < THREAD_CNT; idx++) {
    metainfo_list[idx] = (ThreadMetaInfo*) malloc(sizeof(ThreadMetaInfo));
    metainfo_list[idx]->session = session;
    metainfo_list[idx]->thread_id = idx;
    strcpy(metainfo_list[idx]->table_name, argv[1]);
    strcpy(metainfo_list[idx]->uuid, argv[2]);
    metainfo_list[idx]->start = start_idx;
    metainfo_list[idx]->end = end_idx;
    start_idx = end_idx + 1;
    end_idx = start_idx + range_count;
    pthread_create(&thread_id[idx], NULL, execute_and_log_select_thread, (void *)metainfo_list[idx]);
  }


#if 1
  pthread_t thread_id_2[THREAD_CNT];
  ThreadMetaInfo* metainfo_list_2[THREAD_CNT];
  start_idx = 0;
  end_idx = range_count;
  // insert paralley in other table.
  for (idx = 0; idx < THREAD_CNT; idx++) {
    metainfo_list_2[idx] = (ThreadMetaInfo*) malloc(sizeof(ThreadMetaInfo));
    metainfo_list_2[idx]->session = session;
    metainfo_list_2[idx]->thread_id = idx;
    strcpy(metainfo_list_2[idx]->table_name, new_table);
    strcpy(metainfo_list_2[idx]->uuid, argv[2]);
    metainfo_list_2[idx]->start = start_idx;
    metainfo_list_2[idx]->end = end_idx;
    start_idx = end_idx + 1;
    end_idx = start_idx + range_count;
    pthread_create(&thread_id_2[idx], NULL, execute_and_log_select_thread, (void *)metainfo_list_2[idx]);
  }

  for (idx = 0; idx < THREAD_CNT; idx++) {
    pthread_join(thread_id_2[idx], NULL);
  }


  for (idx = 0; idx < THREAD_CNT; idx++) {
    free(metainfo_list_2[idx]);
  }
#endif

  for (idx = 0; idx < THREAD_CNT; idx++) {
    pthread_join(thread_id[idx], NULL);
  }

  for (idx = 0; idx < THREAD_CNT; idx++) {
    free(metainfo_list[idx]);
  }


  printf("After Thread\n");
  close_future = cass_session_close(session);
  cass_future_wait(close_future);
  cass_future_free(close_future);

  cass_cluster_free(cluster);
  cass_session_free(session);

  return 0;
}
