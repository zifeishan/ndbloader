// #include <iostream>
// #include <fstream>
// #include <sstream>
// #include <iterator>
// #include <vector>
// #include <queue>
// #include <algorithm>
// #include <my_global.h>
// #include <mysql.h>
// #include <my_config.h>
// #include <m_ctype.h>
// #include <NdbApi.hpp>

// #include <decimal.h>

// #include <time.h>
// #include <sys/time.h>
// #include <string.h>
// #include <getopt.h>

// Example

/** 
RUN WITH:
  DYLD_LIBRARY_PATH=/Users/zifei/package/mysql/lib

Example run:
  ./ndbloader 127.0.0.1:1186

*/

#include <stdio.h>
#include <iostream>
#include <fstream>
#include <sstream>
#include <string.h>
#include <stdlib.h>
#include <vector>
#include <unistd.h>

#include <NdbApi.hpp>
#include <mysql.h>
#include <mgmapi.h>

using namespace std;

#define PRINT_ERROR(code,msg) \
  std::cout << "Error in " << __FILE__ << ", line: " << __LINE__ \
            << ", code: " << code \
            << ", msg: " << msg << "." << std::endl
#define MYSQLERROR(mysql) { \
  PRINT_ERROR(mysql_errno(&mysql),mysql_error(&mysql)); \
  exit(-1); }
#define APIERROR(error) { \
  PRINT_ERROR(error.code,error.message); \
  exit(-1); }

#define MAXTRANS 1000000            //max number of outstanding
                                    //transaction in one Ndb object

static char connstring[255] = {};
static char database[255] = {};
static char filepath[255] = {};
static char tablefilepath[255] = {};
static char tablename[255] = {};
static int TRANACTION_SIZE = 10000;

static int tNoOfParallelTrans = 60;   // max no of parallel trans at a time
static int nPreparedTransactions = 0; // a counter

static unsigned int sleepTimeMilli = 20;

// static Ndb* ndb;

// static NdbTransaction* tConArray[MAXTRANS] = {}; // all transactions
// static int conTail = 0; 



/**
 * callback struct.
 * transaction :  index of the transaction in transaction[] array below
 * data : the data that the transaction was modifying.
 * retries : counter for how many times the trans. has been retried
 */
typedef struct  {
  Ndb * ndb;
  int    transaction;  
  // int    data;
  // int    retries;
} async_callback_t;

/**
 * Structure used in "free list" to a NdbTransaction
 */
typedef struct  {
  NdbTransaction*  conn;   
  int used; 
} transaction_t;

transaction_t transaction[MAXTRANS] = {};
int transTail = 0;  // used for optimizing scan

void
closeTransaction(Ndb * ndb , async_callback_t * cb)
{
  ndb->closeTransaction(transaction[cb->transaction].conn);
  transaction[cb->transaction].conn = 0;
  transaction[cb->transaction].used = 0;
  // cb->retries++;  
}

/**
 * Callback executed when transaction has return from NDB
 */
static void
callback(int result, NdbTransaction* NdbObject, void* aObject)
{

  // cerr << (NdbTransaction*)aObject << endl;
  // NdbConnection **array_ref = (NdbConnection**)aObject;
  // assert(NdbObject == *array_ref);
  // *array_ref = NULL;

  async_callback_t * cbData = (async_callback_t *)aObject;

  if (result < 0)
  {
    // cerr << result << endl;
    // cerr << "Error when handling " << cbData->transaction << endl;
    cerr << "Error when handling transaction #" << cbData->transaction << ". MESSAGE: " <<  NdbObject->getNdbError().message << ". Error code = " <<  NdbObject->getNdbError().code << endl;
    // cerr << "Execute: " <<  NdbObject->getNdbError().message << endl;
    // cerr << "Error code = " <<  NdbObject->getNdbError().code << endl;
    delete cbData;
    // asynchExitHandler((Ndb*)cbData->ndb);
    if ((Ndb*)cbData->ndb != NULL)
      delete (Ndb*)cbData->ndb;
    exit(-1);

    // ndbout_c("execute: %s", NdbObject->getNdbError().message);
    // ndbout_c("Error code = %d", NdbObject->getNdbError().code);
  } 
  else 
  {
    /**
     * OK! close transaction
     */
    closeTransaction((Ndb*)cbData->ndb, cbData);
    delete cbData;

    // ndb->closeTransaction(NdbObject);
    // // NdbObject->close(); /* Close transaction */
    // return;
  }
}




Ndb_cluster_connection* connect_to_cluster(char* conn_string);
void disconnect_from_cluster(Ndb_cluster_connection *c);

Ndb_cluster_connection* connect_to_cluster(char* conn_string)
{
  Ndb_cluster_connection* c;

  if(ndb_init())
    exit(EXIT_FAILURE);

  c= new Ndb_cluster_connection(conn_string);

  if(c->connect(4, 5, 1))
  {
    fprintf(stderr, "Unable to connect to cluster within 30 seconds.\n\n");
    exit(EXIT_FAILURE);
  }

  if(c->wait_until_ready(30, 0) < 0)
  {
    fprintf(stderr, "Cluster was not ready within 30 seconds.\n\n");
    exit(EXIT_FAILURE);
  }

  return c; 
}

void disconnect_from_cluster(Ndb_cluster_connection *c)
{
  delete c;

  ndb_end(2);
}

// Insert problems with varchar: use this!
void make_ndb_varchar(char *buffer, const char *str)
{
  int len = strlen(str);
  int hlen = (len > 255) ? 2 : 1;
  buffer[0] = len & 0xff;
  if( len > 255 )
    buffer[1] = (len / 256);
  strcpy(buffer+hlen, str);
}

// Insert problems with varchar: use this!
void make_ndb_char(char *buffer, const char *str)
{
  memset(buffer, 32, sizeof(buffer));
  memcpy(buffer, str, strlen(str));
}

vector<string> fieldName;
vector<string> fieldType;
// char* fieldName[255] = {};
// char* fieldType[255] = {};

void print_help() {
  printf("Usage: ndbloader conn_string database "
    "data_file table_format_file [nParallelTransactions=%d] [milliSleep=%d]", 
    TRANACTION_SIZE, sleepTimeMilli);
}

int main(int argc, char* argv[])
{
  // setlocale(LC_ALL, "");

  if (argc < 2) {
    printf("No connection string specified.\n");
    print_help();
    return -1;
  }
  if (argc < 3) {
    printf("No database specified.\n");
    print_help();
    return -1;
  }
  if (argc < 4) {
    printf("No filepath specified.\n");
    print_help();
    return -1;
  }  
  if (argc < 5) {
    printf("No table format file specified.\n");
    print_help();
    return -1;
  }
  if (argc >= 6) {
    tNoOfParallelTrans = atoi(argv[5]);
  }
  if (argc >= 7) {
    // TRANACTION_SIZE = atoi(argv[6]);
    sleepTimeMilli = atoi(argv[6]);
  }
  // printf("transaction size: %d\n", TRANACTION_SIZE);
  // printf("Number of parallel transactions: %d\n", tNoOfParallelTrans);

  strcpy(connstring, argv[1]);
  strcpy(database, argv[2]);
  strcpy(filepath, argv[3]);
  strcpy(tablefilepath, argv[4]);

  ifstream fin(tablefilepath);
  // first line is table name.
  string tmpTableName;
  getline(fin, tmpTableName);
  strcpy(tablename, tmpTableName.c_str());
  // printf("Copying to table: %s\n", tablename);

  // Initialize transaction array
  for(int i = 0 ; i < MAXTRANS ; i++) 
  {
    transaction[i].used = 0;
    transaction[i].conn = 0;
  }

  // each line is a column
  for (string row; getline(fin, row); ) {
    istringstream lineReader(row);
    string key;
    if( getline(lineReader, key, ' ') )
    {
      string value;
      if( getline(lineReader, value) ) {
        // "key, value"
        fieldName.push_back(key);
        fieldType.push_back(value);
        // printf("Attr: %s %s\n", key.c_str(), value.c_str());
      }
    }
  }
  fin.close();


  // printf("Conn String: %s\n", argv[1]);

  Ndb_cluster_connection *conn = connect_to_cluster(connstring);
  conn->set_timeout(1000000); // 1000s
  // printf("Connection Established. Conn String: %s\n", connstring);

  Ndb* ndb = new Ndb(conn, database);
  if (ndb->init(1024) == -1)
  {
     // what is this?
  }
  ndb->waitUntilReady(10000);
  printf("Connected: database [%s], connstr [%s], #parallelTrans=[%d]. Load table [%s] from file [%s]...\n", database, connstring, tNoOfParallelTrans, tablename, filepath);

  // do_insert(*ndb);

  const NdbDictionary::Dictionary* myDict= ndb->getDictionary();
  // printf("table name: %s\n", tablename);
  const NdbDictionary::Table *myTable= myDict->getTable(tablename);

  if (myTable == NULL) 
    APIERROR(myDict->getNdbError());



  // // DEBUG
  // NdbTransaction *myTransaction= ndb->startTransaction();
  // cout << myTransaction << endl;
  // if (myTransaction == NULL) APIERROR(ndb->getNdbError());
  // for (int i = 0; i < 5; i++) {
  //   printf("Inserting for i=%d\n", i);
    
  //   NdbOperation *myOperation= myTransaction->getNdbOperation(myTable);
    
  //   myOperation->insertTuple();
  //   myOperation->setValue("value", i);
  //   char varcharArr[255] = {};
  //   make_ndb_varchar(varcharArr, to_string(i).c_str());
  //   myOperation->equal("id", varcharArr);
    
  //   if (myTransaction->execute( NdbTransaction::NoCommit ) == -1)
  //     APIERROR(myTransaction->getNdbError());
    
    
  // }

  // if (myTransaction->execute( NdbTransaction::Commit ) == -1)
  //     APIERROR(myTransaction->getNdbError());  
  // ndb->closeTransaction(myTransaction);


  // int primaryKeyIndex = 1;

  // Load the data
  bool dataleft = false;

  typedef vector<vector<string> > Rows;
  // NdbTransaction *myTransaction = ndb->startTransaction();;

  // Rows rows;
  ifstream input(filepath);
  char const row_delim = '\n';
  char const field_delim = '\t';
  int rowCounter = 0;
  for (string row; getline(input, row, row_delim); ) {

    // Find a slot in the transaction array
    async_callback_t * cb;
    // int retries = 0;
    int current = -1;

    int cursor = transTail + 1;
    if (cursor >= MAXTRANS) {
      cursor = 0;
    }
    for(int cursor=0; cursor < MAXTRANS; cursor++) // TODO optimize this
    // while(true)
    {
      // printf("%d %d %a\n", i, transaction[i].used, transaction[i].conn );
      if(transaction[cursor].used == 0)
      {
        // printf("Trans %d\n", i);
        
        current = cursor;
        cb = new async_callback_t;
        /**
         * Set data used by the callback
         */
        cb->ndb = ndb;  //handle to Ndb object so that we can close transaction
                          // in the callback (alt. make myNdb global).

        cb->transaction = current; //This is the number (id)  of this transaction
        transaction[current].used = 1 ; //Mark the transaction as used
        transTail = current; // optimizing scan

        break;
      }
      else { // used
        cursor += 1; 
        if (cursor >= MAXTRANS) {
          cursor = 0;
        }

      }
    }
    if(current == -1) {
      cerr << "FATAL: number of transactions in parallel exceeds the maximum. Terminating." << endl;
      return -1;
    }


    // DEBUG
    // printf("\n");
    transaction[current].conn = ndb->startTransaction();

    istringstream ss(row);
    // NdbOperation *myOperation= myTransaction->getNdbOperation(myTable);
    NdbOperation *myOperation= transaction[current].conn->getNdbOperation(myTable);
    myOperation->insertTuple();

    // Iterate for each field
    int i = 0;
    for (string field; getline(ss, field, field_delim); i++) {

      // DEBUG
      // printf("%d %s\t", i, field.c_str());

      if (strcmp(fieldType[i].c_str(), "int") == 0) {
        // using a int64 to prevent problems..
        long long value = atoll(field.c_str());
        myOperation->setValue(fieldName[i].c_str(), value);
      }

      if (strcmp(fieldType[i].c_str(), "real") == 0) {
        double value = atof(field.c_str());
        myOperation->setValue(fieldName[i].c_str(), value);    
      }
      
      if (strcmp(fieldType[i].c_str(), "varchar") == 0) {
        char buffer[65535] = {};
        make_ndb_varchar(buffer, field.c_str());
        myOperation->setValue(fieldName[i].c_str(), buffer);
      }
      
      if (strcmp(fieldType[i].c_str(), "char") == 0) {
        char buffer[65535] = {};
        make_ndb_char(buffer, field.c_str());
        myOperation->setValue(fieldName[i].c_str(), buffer);
        // myOperation->setValue(fieldName[i].c_str(), field.c_str());
      }

      if (strcmp(fieldType[i].c_str(), "text") == 0) {
        NdbBlob *myBlobHandle = myOperation->getBlobHandle(fieldName[i].c_str());
        if (myBlobHandle == NULL) {
          cerr << "Hint: in the TSV file any TEXT/BLOB attribute must come after the primary key column.\n";
          APIERROR(myOperation->getNdbError());
        }
        myBlobHandle->setValue(field.c_str(), field.length());
        // myBlobHandle->setNull();
      }

    }

    transaction[current].conn->executeAsynchPrepare( NdbTransaction::Commit, 
                                         &callback, 
                                         cb
                                         );
    // conTail++;
    // if (conTail == MAXTRANS)
    //   conTail = 0;
    nPreparedTransactions++;
    rowCounter++;
    dataleft = true;
    /**
     * When we have prepared parallelism number of transactions ->
     * send the transaction to ndb. 
     * Next time we will deal with the transactions are in the 
     * callback. There we will see which ones that were successful
     * and which ones to retry.
     */
    if (nPreparedTransactions >= tNoOfParallelTrans)
    {
      // send-poll all transactions
      // close transaction is done in callback
      ndb->sendPollNdb(3000, tNoOfParallelTrans );
      nPreparedTransactions=0;
      dataleft = false;
      
      usleep(sleepTimeMilli);

    }

    // SYNC
    // if (myTransaction->execute( NdbTransaction::NoCommit ) == -1)
    //   APIERROR(myTransaction->getNdbError());

    // DEPRECATED
    // if (myTransaction->execute( NdbTransaction::Commit ) == -1)
    //   APIERROR(myTransaction->getNdbError());
    // ndb->closeTransaction(myTransaction);
    // myTransaction = ndb->startTransaction();


    
    // if (rowCounter % TRANACTION_SIZE == 0) {
    //   // commit
    //   if (myTransaction->execute( NdbTransaction::Commit ) == -1)
    //     APIERROR(myTransaction->getNdbError());
    //   ndb->closeTransaction(myTransaction);
    //   myTransaction = ndb->startTransaction();
    //   dataleft = false;
    // }

  }

  if (dataleft) {
    ndb->sendPollNdb(3000, nPreparedTransactions );
    nPreparedTransactions=0;
      

    // if (myTransaction->execute( NdbTransaction::Commit ) == -1)
    //   APIERROR(myTransaction->getNdbError());
    // ndb->closeTransaction(myTransaction);    
  }



  /**
  // operation
  for (countAttributes = 1;
           countAttributes < loopCountAttributes; countAttributes++) {
        localNdbOperation->setValue(countAttributes + 1,
                                    (char*)&attrValue[0]);
      }//for
  // record operation
  op = pTrans->insertTuple(g_record, record, g_record, record);
  */


  ndb->waitUntilReady(10000);

  delete ndb;
  // disconnect_from_cluster(conn);

  return EXIT_SUCCESS;
}
