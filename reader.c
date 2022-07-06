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

CassError execute_and_log_select(CassSession* session, const char* stmt) {
  CassError rc = CASS_OK;
  CassFuture* future = NULL;
  CassStatement* statement = cass_statement_new(stmt, 0);

  future = cass_session_execute(session, statement);
  rc = cass_future_error_code(future);
  if (rc != CASS_OK) {
    print_error(future);
  } else {
    const CassResult* result = cass_future_get_result(future);
    CassIterator* iterator = cass_iterator_from_result(result);
    if (cass_iterator_next(iterator)) {
      const CassRow* row = cass_iterator_get_row(iterator);
      const char* k; size_t k_length;
      const char* v; size_t v_length;
      cass_value_get_string(cass_row_get_column(row, 0), &k, &k_length);
      cass_value_get_string(cass_row_get_column(row, 1), &v, &v_length);
      printf ("Thread: Select statement returned: Row[%.*s, %.*s]\n", (int)k_length, k, (int)v_length, v);
    } else {
      printf("Thread: Unable to fetch row!\n");
    }

    cass_result_free(result);
    cass_iterator_free(iterator);
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
  int idx = 0;
  int max_idx_val = 50000000;
  //int max_idx_val = 50;
 // add table name and uuid
  while (1) {


  char select_stmt[BUFFER_SIZE] = {0};
  if (idx > max_idx_val) {
     idx = 0;
  }

  //snprintf(select_stmt, 500, "SET yb_read_from_followers = true");
  //execute_query(session, select_stmt);

  //snprintf(select_stmt, 500, "START TRANSACTION ISOLATION LEVEL READ UNCOMMITTED");
  //execute_query(session, select_stmt);
  //6158934f-2675-4055-912d-3996f89fb0aa
  //ybdemo_keyspace.test1
  #ifdef UNIVERSE_01
  snprintf(select_stmt, BUFFER_SIZE, "SELECT k, v from %s  WHERE k = '%s:k:%d'", thread_info->table_name, thread_info->uuid, idx);
  #else
  snprintf(select_stmt, BUFFER_SIZE, "SELECT hrtid, eventid, asof, effective, effective_end, sticky, hides_hrtid, validates_hrtid,hrt_symbol, updates_hrt_symbol, listed, updates_listed, listing_mic, updates_listing_mic, currency, updates_currency, isin, updates_isin,cusip, updates_cusip, sedol, updates_sedol, bb_melded_name, updates_bb_melded_name, bb_exch_symbol, updates_bb_exch_symbol,bb_composite_exch_code, updates_bb_composite_exch_code, bb_unique_id, updates_bb_unique_id, bb_region, updates_bb_region, figi, updates_figi,composite_figi, updates_composite_figi, share_class_figi, updates_share_class_figi, bb_global_company_id, updates_bb_global_company_id,recrunch_tag, bb_venue_figis_and_mics from %s WHERE bb_global_company_id='bb_global_company_id-%d'", thread_info->table_name, idx);
  #endif
  printf("fetching idx: %d\n", idx);
  CassStatement* statement = cass_statement_new(select_stmt, 0);
  cass_statement_set_consistency(statement, CASS_CONSISTENCY_ONE); // CASS_CONSISTENCY_ALL
  future = cass_session_execute(session, statement);
  rc = cass_future_error_code(future);
  if (rc != CASS_OK) {
    print_error(future);
    if (rc == CASS_ERROR_LIB_NO_HOSTS_AVAILABLE) {
       break;
    }

  } else {
    printf("executing from thread......\n");
    const CassResult* result = cass_future_get_result(future);
    CassIterator* iterator = cass_iterator_from_result(result);
    if (cass_iterator_next(iterator)) {
      const CassRow* row = cass_iterator_get_row(iterator);
      int age;
      const char* k; size_t k_length;
      const char* v; size_t v_length;
      cass_value_get_string(cass_row_get_column(row, 0), &k, &k_length);
      cass_value_get_string(cass_row_get_column(row, 1), &v, &v_length);
      printf ("Thread: Select statement returned: Row[%.*s, %.*s]\n", (int)k_length, k, (int)v_length, v);
      idx += 1;
    } else {
      printf("Thread: Unable to fetch row!\n");
      sleep(3);
    }

    cass_result_free(result);
    cass_iterator_free(iterator);
  }
  cass_future_free(future);
  cass_statement_free(statement);
}

}

int main(int argc, char** argv) {
  // Ensure you log errors.
  cass_log_set_level(CASS_LOG_INFO);

  CassCluster* cluster = NULL;
  CassSession* session = cass_session_new();
  CassFuture* close_future = NULL;
  //char* hosts = "127.0.0.1";
  #ifdef UNIVERSE_01
     char* hosts = "10.150.5.72,10.150.5.73,10.150.5.84";
  #else
      //char* hosts = "10.150.1.40,10.150.1.37,10.150.1.35";
      char* hosts = "10.150.5.72,10.150.5.84,10.150.5.73";
      //char* hosts = "10.150.5.73";
  #endif

  if (argc <= 1) {
   fprintf(stderr, "provide <exe-name> <table-name> <uuid>\n");
   return -1;
  }

  cluster = create_cluster(hosts);
  if (connect_session(session, cluster) != CASS_OK) {
    cass_cluster_free(cluster);
    cass_session_free(session);
    return -1;
  }
  cass_cluster_set_load_balance_dc_aware(cluster, "us-west1", 0, cass_false);

  CassError rc = CASS_OK;

#define THREAD_CNT  50
  pthread_t thread_id[THREAD_CNT];
  ThreadMetaInfo* metainfo_list[THREAD_CNT];

  int idx = 0;
#if 1
  //SET yb_read_from_followers = true
  char query[BUFFER_SIZE] = {0};
  //snprintf(query, 500, "SET yb_read_from_followers = true");
  //execute_query(session, query);

  //snprintf(query, 500, "START TRANSACTION ISOLATION LEVEL READ UNCOMMITTED");
  //execute_query(session, query);

  printf("Before Thread\n");
  for (; idx < THREAD_CNT; idx++) {
      metainfo_list[idx] = (ThreadMetaInfo*) malloc(sizeof(ThreadMetaInfo));
      metainfo_list[idx]->session = session;
      metainfo_list[idx]->thread_id = idx;
      strcpy(metainfo_list[idx]->table_name, argv[1]);
      strcpy(metainfo_list[idx]->uuid, argv[2]);
      pthread_create(&thread_id[idx], NULL, execute_and_log_select_thread, (void *)metainfo_list[idx]);
  }

  for (idx = 0; idx < THREAD_CNT; idx++) {
     pthread_join(thread_id[idx], NULL);
  }

  for (idx = 0; idx < THREAD_CNT; idx++) {
      free(metainfo_list[idx]);
  }
  printf("After Thread\n");
#endif
#if 0

  const char* select_stmt = "SELECT k, v from ybdemo_keyspace.test1 WHERE k = '6158934f-2675-4055-912d-3996f89fb0aa:127'";
  rc = execute_and_log_select(session, select_stmt);
  if (rc != CASS_OK) return -1;
#endif
  close_future = cass_session_close(session);
  cass_future_wait(close_future);
  cass_future_free(close_future);

  cass_cluster_free(cluster);
  cass_session_free(session);

  return 0;
}
