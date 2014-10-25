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
#include <NdbApi.hpp>
#include <mysql.h>
#include <mgmapi.h>
#include <vector>
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

char connstring[255] = {};
char database[255] = {};
char filepath[255] = {};
char tablefilepath[255] = {};
char tablename[255] = {};
int TRANACTION_SIZE = 10000;



// global record
static NdbRecord * g_record;

static void do_insert(Ndb &myNdb)
{
  const NdbDictionary::Dictionary* myDict= myNdb.getDictionary();
  const NdbDictionary::Table *myTable= myDict->getTable("api_simple");

  if (myTable == NULL) 
    APIERROR(myDict->getNdbError());

  for (int i = 0; i < 5; i++) {
    NdbTransaction *myTransaction= myNdb.startTransaction();
    if (myTransaction == NULL) APIERROR(myNdb.getNdbError());
    
    NdbOperation *myOperation= myTransaction->getNdbOperation(myTable);
    if (myOperation == NULL) APIERROR(myTransaction->getNdbError());
    
    myOperation->insertTuple();
    myOperation->equal("ATTR1", i);
    myOperation->setValue("ATTR2", i);

    myOperation= myTransaction->getNdbOperation(myTable);
    if (myOperation == NULL) APIERROR(myTransaction->getNdbError());

    myOperation->insertTuple();
    myOperation->equal("ATTR1", i+5);
    myOperation->setValue("ATTR2", i+5);
    
    if (myTransaction->execute( NdbTransaction::Commit ) == -1)
      APIERROR(myTransaction->getNdbError());
    
    myNdb.closeTransaction(myTransaction);
  }
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



int main(int argc, char* argv[])
{
  setlocale(LC_ALL, "");

  if (argc < 2) {
    printf("No connection string specified.");
    return -1;
  }
  if (argc < 3) {
    printf("No database specified.");
    return -1;
  }
  if (argc < 4) {
    printf("No filepath specified.");
    return -1;
  }  
  if (argc < 5) {
    printf("No table format file specified.");
    return -1;
  }
  if (argc >= 6) {
    TRANACTION_SIZE = atoi(argv[5]);
  }
  printf("transaction size: %d\n", TRANACTION_SIZE);

  strcpy(connstring, argv[1]);
  strcpy(database, argv[2]);
  strcpy(filepath, argv[3]);
  strcpy(tablefilepath, argv[4]);

  ifstream fin(tablefilepath);
  // first line is table name.
  string tmpTableName;
  getline(fin, tmpTableName);
  strcpy(tablename, tmpTableName.c_str());
  printf("Copying to table: %s\n", tablename);

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
        printf("Attr: %s %s\n", key.c_str(), value.c_str());
      }
    }
  }
  fin.close();


  printf("Conn String: %s\n", argv[1]);

  Ndb_cluster_connection *conn = connect_to_cluster(connstring);

  printf("Connection Established. Conn String: %s\n", connstring);

  Ndb* ndb = new Ndb(conn, database);
  if (ndb->init(1024) == -1)
  {
     // what is this?
  }
  ndb->waitUntilReady(10000);
  printf("Connected to database: %s\n", database);  

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
  NdbTransaction *myTransaction = ndb->startTransaction();;
  // Rows rows;
  ifstream input(filepath);
  char const row_delim = '\n';
  char const field_delim = '\t';
  int rowCounter = 0;
  for (string row; getline(input, row, row_delim); ) {

    istringstream ss(row);
    NdbOperation *myOperation= myTransaction->getNdbOperation(myTable);
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
      
      // if (i == primaryKeyIndex) {
      //   // FOR VARCHAR
        
      // } else {
      //   // FOR INT
        
      // }
      

    }

    // DEBUG
    // printf("\n");
    
    if (myTransaction->execute( NdbTransaction::NoCommit ) == -1)
      APIERROR(myTransaction->getNdbError());
    // if (myTransaction->execute( NdbTransaction::Commit ) == -1)
    //   APIERROR(myTransaction->getNdbError());
    // ndb->closeTransaction(myTransaction);
    // myTransaction = ndb->startTransaction();


    rowCounter++;
    dataleft = true;
    if (rowCounter % TRANACTION_SIZE == 0) {
      // commit
      if (myTransaction->execute( NdbTransaction::Commit ) == -1)
        APIERROR(myTransaction->getNdbError());
      ndb->closeTransaction(myTransaction);
      myTransaction = ndb->startTransaction();
      dataleft = false;
    }

  }

  if (dataleft) {
    if (myTransaction->execute( NdbTransaction::Commit ) == -1)
      APIERROR(myTransaction->getNdbError());
    ndb->closeTransaction(myTransaction);    
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




  delete ndb;
  disconnect_from_cluster(conn);

  return EXIT_SUCCESS;
}