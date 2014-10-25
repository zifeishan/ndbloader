CFLAGS="$CFLAGS "`mysql_config --cflags`
CFLAGS="$CFLAGS "`mysql_config --include`/storage/ndb
CFLAGS="$CFLAGS "`mysql_config --include`/storage/ndb/ndbapi
CFLAGS="$CFLAGS "`mysql_config --include`/storage/ndb/mgmapi
# older than 6.3.14
#LDFLAGS="$LDFLAGS "`mysql_config --libs_r`
#LDFLAGS="$LDFLAGS -lndbclient -lmysys -lmystrings"
# Beginning with MySQL Cluster NDB 6.2.16 and MySQL Cluster NDB 6.3.14, it is necessary only to add -lndbclient to LD_FLAGS, as shown here:
LDFLAGS="$LDFLAGS "`mysql_config --libs_r`
LDFLAGS="$LDFLAGS -lndbclient"
SYS_LIB="-lpthread -lm "

g++ -c ndbloader.cpp $CFLAGS
g++ ndbloader.o -o ndbloader $LDFLAGS $SYS_LIB
 # -Wl,-rpath  -Wl,/Users/zifei/package/mysql/lib
# g++ -o ndbloader $CFLAGS $LDFLAGS $SYS_LIB

case $(uname) in
  Darwin)
    echo "RUN: DYLD_LIBRARY_PATH="`mysql_config --variable=pkglibdir`" ./ndbloader [connection string]"
    ;;

  Linux*)
    echo "RUN: LD_LIBRARY_PATH="`mysql_config --variable=pkglibdir`" ./ndbloader [connection string]"
    ;;

  *)
    echo >&2 "$(uname): Unsupported OS"
    false
esac

