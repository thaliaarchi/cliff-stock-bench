package bw2;

/* @2018 Rocket Realtime School of Programming and Performance
 *
 *  Goal: Understand the cost of (re)visiting Big Data
 *
 *  Technique: Compare running time to load and analyze a 1.8G file (of stock
 *  ticker data).

 *  Driver 1: Load using a simple CSV reader, making one pass per analytic over all data
 *  Driver 2: Load using a simple CSV reader, doing all the analytics in one pass
 *  Driver 3: Load as lines, no CSV, only parse fields as needed
 *  Driver 4: Load as bytes via FileInputStream; parse fields as needed into Strings
 *  Driver 5: Load as bytes via FileInputStream; parse fields as needed into shared Strs
 *
 *  Running on a 3.6GHz Sandybridge with 4/8 cores, 256K L2, and 10M shared L3.
 *
 *    lines  Driver1   Driver2    Driver3    Driver4    Driver5    
 *    10000  31KL/sec   56KL/sec   85KL/sec  110KL/sec  138KL/sec
 *   100000  72KL/sec  155KL/sec  250KL/sec  460KL/sec  530KL/sec
 *  1000000  85KL/sec  245KL/sec  425KL/sec  895KL/sec 1043KL/sec
 *  2850767  88KL/sec  257KL/sec  454KL/sec 1031KL/sec 1182KL/sec
 *
 * Observing that total running times are getting very short, so...
 * Running 5 times and taking the max:
 *  2850767  90KL/sec  273KL/sec  494KL/sec 1097KL/sec 1331KL/sec
 *  Ratios:   0.33       1.0        1.8        4.0        4.8
 */
import bw2.UtilUnsafe.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.Future;

import static bw2.UtilUnsafe.UNSAFE;
import static bw2.UtilUnsafe.fieldOffset;

abstract class Bandwidth {
  public static void main( String[] args ) throws Exception {
    main_driver( Integer.valueOf(args[0]),  Integer.valueOf(args[1]) );
    //main_driver( Integer.valueOf(args[0]),  Integer.valueOf(args[1]) );
    //main_driver( Integer.valueOf(args[0]),  Integer.valueOf(args[1]) );
    //main_driver( Integer.valueOf(args[0]),  Integer.valueOf(args[1]) );
    //main_driver( Integer.valueOf(args[0]),  Integer.valueOf(args[1]) );
  }

  static private void main_driver( int driver, int N ) throws Exception {
    long t0 = System.nanoTime();
    ConcurrentHashMap<Object,ProdData> prods = null;
    NonBlockingHashMapLong<ProdData> prods_l = null;
    switch( driver ) {
    case 1: prods = driver1(N); break;
    case 2: prods = driver2(N); break;
    case 3: prods = driver3(N); break;
    case 4: prods = driver4(N); break;
    case 5: prods = driver5(N); break;
    case 6: prods_l = driver6(N); break;
    default: throw unimpl();
    }
    long t1 = System.nanoTime();
    //ProdData meta = prods!=null ? prods.get(",meta") : prods_l.get(str2long(",meta"));
    //long lines = meta._cnt;
    //long bytes = meta._tot_qty;
    //double secs = (double)(t1-t0)/1e9;
    //System.out.printf("driver%d: %6.0f lines/sec; %6.2f Mbytes/sec; %d lines; %6.3f secs\n",
    //                  driver,lines/secs,(bytes/1e6/secs), lines, secs);
    Collection<ProdData> cprods = prods==null ? prods_l.values() : prods.values();
    //ProdData[] aprods = cprods.toArray(new ProdData[0]);
    //Arrays.sort(aprods, (p0,p1) -> p0._product.compareTo(p1._product));
    for( ProdData prod : cprods )
      System.out.println(prod);
  }

  // ------------------------------------------------------
  // Read the data as lines, split into Strings on-the-fly and do the math.
  // One pass per analytic.
  static private ConcurrentHashMap<Object,ProdData> driver1( int N ) throws IOException {
    ConcurrentHashMap<Object,ProdData> prods = new ConcurrentHashMap<>();

    // For each product, count records
    CSV csv = new CSV(file());
    int lines=N;
    for( int i=0; i<N; i++ ) {
      String[] row = csv.readLineAndSplit();
      if( row == null ) { lines=i; break; }
      String source = row[csv._idx_src ];
      String prod   = row[csv._idx_prod];
      if( !filter(source) ) continue; // Filter out this row
      prods.computeIfAbsent(prod,ProdData::new).cnt(); // Count actions
    }

    // For each product, count buys and sells
    csv = new CSV(file());
    for( int i=0; i<lines; i++ ) {
      String[] row = csv.readLineAndSplit();
      String source = row[csv._idx_src];
      String prod   = row[csv._idx_prod];
      String b_s    = row[csv._idx_bs  ];
      if( !filter(source) ) continue; // Filter out this row
      prods.get(prod).buy_sell(b_s); // Count buys and sells
    }

    // For each product, count max quantities bought and sold
    csv = new CSV(file());
    for( int i=0; i<lines; i++ ) {
      String[] row = csv.readLineAndSplit();
      String source = row[csv._idx_src];
      String prod   = row[csv._idx_prod];
      String ordqty = row[csv._idx_ordqty];
      String wrkqty = row[csv._idx_wrkqty];
      String excqty = row[csv._idx_excqty];
      if( !filter(source) ) continue; // Filter out this row
      prods.get(prod).max_qty(ordqty,wrkqty,excqty); // Count quantities
    }
    
    // Add a bogus sentinel to return meta-data
    //ProdData data = prods.computeIfAbsent(",meta",ProdData::new);
    //data._cnt = lines;
    //data._tot_qty = csv._len;   // Total length read
    return prods;
  }
  // ------------------------------------------------------
  // ------------------------------------------------------
  // Read the data as lines, split into Strings on-the-fly and do the math.
  // One pass for all analytics - the file is read only once, although the data
  // is copied from file buffers to a String line, and then again when
  // String.split() is called (minimum of 3 copies of the data).  Each of the
  // copies is into freshly allocated memory (and later GCd).
  static private ConcurrentHashMap<Object,ProdData> driver2( int N ) throws IOException {
    ConcurrentHashMap<Object,ProdData> prods = new ConcurrentHashMap<>();

    // For each product, count records
    CSV csv = new CSV(file());
    int lines=N;
    for( int i=0; i<N; i++ ) {
      String[] row = csv.readLineAndSplit();
      if( row == null ) { lines=i; break; }
      String source = row[csv._idx_src ];
      String prod   = row[csv._idx_prod];
      String b_s    = row[csv._idx_bs  ];
      String ordqty = row[csv._idx_ordqty];
      String wrkqty = row[csv._idx_wrkqty];
      String excqty = row[csv._idx_excqty];
      if( !filter(source) ) continue; // Filter out this row
      ProdData data = prods.computeIfAbsent(prod,ProdData::new);
      data.cnt();               // Count actions
      data.buy_sell(b_s);       // Count buys and sells
      data.max_qty(ordqty,wrkqty,excqty); // Count quantities
    }
    
    // Add a bogus sentinel to return meta-data
    //ProdData data = prods.computeIfAbsent(",meta",ProdData::new);
    //data._cnt = lines;
    //data._tot_qty = csv._len;   // Total length read
    return prods;
  }
  // ------------------------------------------------------
  // ------------------------------------------------------
  // Read the data as lines, find the fields on-the-fly and do the math.  One
  // pass for all analytics - but also the data is less: only the handful of
  // used fields are copied twice into fresh memory, although still all the
  // data is copied once into fresh String lines.
  static private ConcurrentHashMap<Object,ProdData> driver3( int N ) throws IOException {
    ConcurrentHashMap<Object,ProdData> prods = new ConcurrentHashMap<>();

    // For each product, count records
    CSV csv = new CSV(file());
    int lines=N;
    for( int i=0; i<N; i++ ) {
      String line = csv.readLineForCols();
      if( line == null ) { lines=i; break; }
      String source = csv.col(csv._idx_src   );
      if( !filter(source) ) continue; // Filter out this row
      String b_s    = csv.col(csv._idx_bs    );
      String ordqty = csv.col(csv._idx_ordqty);
      String wrkqty = csv.col(csv._idx_wrkqty);
      String excqty = csv.col(csv._idx_excqty);
      String prod   = csv.col(csv._idx_prod  );
      ProdData data = prods.computeIfAbsent(prod,ProdData::new);
      data.cnt();               // Count actions
      data.buy_sell(b_s);       // Count buys and sells
      data.max_qty(ordqty,wrkqty,excqty); // Count quantities
    }
    
    // Add a bogus sentinel to return meta-data
    //ProdData data = prods.computeIfAbsent(",meta",ProdData::new);
    //data._cnt = lines;
    //data._tot_qty = csv._len;   // Total length read
    return prods;
  }
  
  // Only keep rows with Source of "ToClnt" (which means: to the client
  // Trader's gateway from the Exchange).  Other values indicate other
  // directions of data flow, but are otherwise simple replications.
  // In an actual application about 10 tests have pass.
  private static boolean filter( String source ) {
    return source.equals("ToClnt");
  }
  private static boolean filter( Str source ) {
    return source.equals("ToClnt");
  }

  
  // Struct to hold stats per-product.  In an actual application about 200
  // basic stats are gathered.
  static class ProdData {
    String _product;
    ProdData( Object prod ) { _product = prod.toString(); }
    ProdData( long  xprod ) { _product = long2str(xprod); }
    long _cnt;                  // Count of records
    int _buys, _sells;          // Buy/Sell transactions
    long _tot_qty;              // Just the max qtys, totaled for an average
    
    void cnt() { _cnt++; }
    
    void buy_sell( String bs ) {
      if( bs.equals("Buy" ) ) _buys++;
      if( bs.equals("Sell") ) _sells++;
    }
    void buy_sell( Str bs ) {
      if( bs.equals("Buy" ) ) _buys++;
      if( bs.equals("Sell") ) _sells++;
    }
    void buy_sell( boolean buy ) {
      if( buy ) _buys++;
      else      _sells++;
    }
    
    void max_qty( String ordqty, String wrkqty, String excqty ) {
      // Max order/work/exec qty
      int ord = Integer.parseInt(ordqty);
      int wrk = Integer.parseInt(wrkqty);
      int exc = Integer.parseInt(excqty);
      // Total qty involved in the order
      _tot_qty += Math.max(ord,Math.max(wrk,exc));
    }
    void max_qty( Str ordqty, Str wrkqty, Str excqty ) {
      // Max order/work/exec qty
      int ord = ordqty.parseInt();
      int wrk = wrkqty.parseInt();
      int exc = excqty.parseInt();
      // Total qty involved in the order
      _tot_qty += Math.max(ord,Math.max(wrk,exc));
    }
    void max_qty( int ord, int wrk, int exc ) {
      // Total qty involved in the order
      _tot_qty += Math.max(ord,Math.max(wrk,exc));
    }
    @Override public String toString() {
      return String.format("%3s cnt=%4d buy=%4d sell=%4d avg qty=%6.2f",_product,_cnt,_buys,_sells,(double)_tot_qty/_cnt);
    }
  }

  // --- CSV Reader
  private static File file() { return new File("ANON2.csv"); }
  //private static File file() { return new File("ANON1.csv"); }
  
  static class CSV {
    final BufferedReader _br;
    final int _idx_src;         // Index of Source column
    final int _idx_prod;        // Index of Prod   column
    final int _idx_bs;          // Index of B/S    column
    final int _idx_ordqty;      // Index of OrdQty column
    final int _idx_wrkqty;      // Index of WrkQty column
    final int _idx_excqty;      // Index of ExcQty column
    long _len;                  // Approx length read
    String _line;               // Most recent line read
    
    CSV( File f ) throws IOException {
      _br = new BufferedReader(new FileReader(f));
      // Read the header line, and pick out the columns of interest
      String[] headers = readLineAndSplit(); // Read header
      _idx_src   = find(headers,"Source");
      _idx_bs    = find(headers,"B/S"   );
      _idx_ordqty= find(headers,"OrdQty");
      _idx_wrkqty= find(headers,"WrkQty");
      _idx_excqty= find(headers,"ExcQty");
      _idx_prod  = find(headers,"Prod"  );
    }

    // Read a single line as a string.  Returns null at EOF.  Counts total
    // string length read.
    String readLine() throws IOException {
      _line = _br.readLine();
      if( _line != null ) _len += _line.length()+1;
      return _line;
    }
    
    // Return 1 line, broken into strings on commas.
    // Returns null at EOF.
    String[] readLineAndSplit() throws IOException {
      return readLine() == null ? null : _line.split(",");
    }


    // API for doing column-at-a-time
    int _col, _idx;
    // Return 1 line
    String readLineForCols() throws IOException { _col=0; _idx=0; return readLine(); }
    // Read column N, only reading forwards
    String col( int n ) {
      assert _col <= n;
      while( _col < n ) {
        _idx = _line.indexOf(',',_idx)+1;
        _col++;
      }
      int start = _idx;
      _idx = _line.indexOf(',',_idx)+1;
      _col++;
      return _line.substring(start,_idx-1);
    }
    private static <E> int find( E[] ary, E e ) {
      for( int i=0; i<ary.length; i++ )
        if( ary[i].equals(e) )
          return i;
      return -1;
    }
  }

  // ------------------------------------------------------
  // ------------------------------------------------------
  // "No-New-Per-Byte/Line-Read".

  // This driver avoids the main string allocation that happen per-Big-Data
  // item.  This adds the requirement to NOT use a "Reader" subclass - all
  // Readers do character conversion and then typically produce a String -
  // which is explicitly the step we are trying to avoid.
    
  // Read the data as raw bytes, find the fields on-the-fly and do the math.
  // One pass for all analytics - and only copy needed fields into strings.
  
  static private ConcurrentHashMap<Object,ProdData> driver4( int N ) throws IOException {
    ConcurrentHashMap<Object,ProdData> prods = new ConcurrentHashMap<>();

    try( RawCSV csv = new RawCSV(file()) ) {
      int lines=N;
      for( int i=0; i<N; i++ ) {
        if( !csv.skipToLineStart() ) { lines=i; break; } // Skip rest of prior line; align new line
        String source = csv.rawcol(csv._idx_src   );
        if( !filter(source) ) continue; // Filter out this row
        String b_s    = csv.rawcol(csv._idx_bs    );
        String ordqty = csv.rawcol(csv._idx_ordqty);
        String wrkqty = csv.rawcol(csv._idx_wrkqty);
        String excqty = csv.rawcol(csv._idx_excqty);
        String prod   = csv.rawcol(csv._idx_prod  );
        ProdData data = prods.computeIfAbsent(prod,ProdData::new);
        data.cnt();               // Count actions
        data.buy_sell(b_s);       // Count buys and sells
        data.max_qty(ordqty,wrkqty,excqty); // Count quantities
      }
      
      // Add a bogus sentinel to return meta-data
      //ProdData data = prods.computeIfAbsent(",meta",ProdData::new);
      //data._cnt = lines;
      //data._tot_qty = csv._len;   // Total length read
      return prods;
    }
  }

  // ------------------------------------------------------
  // ------------------------------------------------------
  // "No-New-Per-Byte/Line-Read".

  // This driver avoids all string allocation that happen per-Big-Data item. 
  static private ConcurrentHashMap<Object,ProdData> driver5( int N ) throws IOException {
    ConcurrentHashMap<Object,ProdData> prods = new ConcurrentHashMap<>();
    Str src = new Str(), sb_s = new Str(), sord = new Str(), swrk = new Str(), sexc = new Str(), sprd = new Str();
      
    // For each product, count records
    try( RawCSV csv = new RawCSV(file()); ) {
      int lines=N;
      for( int i=0; i<N; i++ ) {
        if( !csv.skipToLineStart() ) { lines=i; break; } // Skip rest of prior line; align new line
        csv.rawcol(src,csv._idx_src   );
        if( !filter(src) ) continue; // Filter out this row
        csv.rawcol(sb_s,csv._idx_bs    );
        csv.rawcol(sord,csv._idx_ordqty);
        csv.rawcol(swrk,csv._idx_wrkqty);
        csv.rawcol(sexc,csv._idx_excqty);
        csv.rawcol(sprd,csv._idx_prod  );
        ProdData data = prods.get(sprd);
        if( data == null ) {
          prods.put(sprd.compact(), data = new ProdData(sprd));
          sprd= new Str();
        }
        data.cnt();                   // Count actions
        data.buy_sell(sb_s);          // Count buys and sells
        data.max_qty(sord,swrk,sexc); // Count quantities
      }
      
      // Add a bogus sentinel to return meta-data
      //ProdData data = prods.computeIfAbsent(",meta",ProdData::new);
      //data._cnt = lines;
      //data._tot_qty = csv._len;   // Total length read
      return prods;
    }
  }
  
  // ------------------------------------------------------
  static class RawCSV extends CSV implements AutoCloseable {
    FileInputStream _fis;
    int _pos, _lim, _eol;
    byte[] _buf;
    RawCSV( File f ) throws IOException {
      super(f);                 // Read and parse file header; 
      _len=0;                   // Remove counts already read by super()
      _br.close();              // Having read the headers, close the BR
      _fis=new FileInputStream(f);
      _buf = new byte[32*1024];
      fill();
    }

    @Override public void close() throws IOException { _fis.close(); }
    
    // Fill the bytebuffer.  Guaranteed to be large enough to hold the largest
    // line (but maybe the line needs to be slid about to avoid a buffer crossing).
    boolean fill() throws IOException {
      // Copy partial line to buffer start
      System.arraycopy(_buf,_pos,_buf,0,_lim-_pos);
      _lim = _lim-_pos;
      _pos = 0;
      // Fill remaining buffer
      while( _lim < _buf.length ) {
        int len = _fis.read(_buf,_lim,_buf.length-_lim);
        if( len == -1 ) return _lim!=0;
        _lim += len;
        _len += len;
      }
      return true;
    }

    // Skip rest of line (guarenteed to be in the buffer).
    // Make sure the entire next line is in buffer.
    boolean skipToLineStart() throws IOException {
      _col=0;                   // Reset back to column 0
      int pos = _pos, lim=_lim; // Load pos,lim into a local variables
      byte[] buf = _buf;        // Same for buf
      if( _eol != 0 ) pos = _eol; // Have pre-computed line end; just skip ahead
      else
        // Find line end.  Guarenteed rest of line is in buffer; no buffer-end check
        while( buf[pos++] != '\n' ) ;
      
      // Test to see if the NEW line is entirely in the buffer
      _pos = pos;               // Set position update back in
      while( pos < lim && buf[pos++] != '\n' ) ;
      if( pos<lim ) { _eol = pos; return true; } // Have a line, record where it ends
      _eol = 0;                                  // Do not know where line ends
      return fill(); // Copy/compact partial line to start of buffer, and fill more into buffer
    }

    // Read raw bytes for column N, only reading forwards.
    // Guarenteed rest of line is in buffer; no end check
    String rawcol( int n ) throws IOException {
      int cpos = rawcol_impl(n);
      return new String(_buf,cpos,_pos-cpos-1);
    }
    Str rawcol( Str s, int n ) throws IOException {
      int cpos = rawcol_impl(n);
      return s.set(_buf,cpos,_pos-cpos-1);
    }
    // Read raw bytes for column N, only reading forwards.
    // Guarenteed rest of line is in buffer; no end check
    int rawcol_impl( int n ) throws IOException {
      assert _col <= n;
      int pos = _pos;           // Load pos into a local variable
      byte[] buf = _buf;        // Same for buf
      while( _col < n ) {
        while( buf[pos++] != ',' ) ;
        _col++;                 // Found a column end
      }
      int cpos = pos;           // Column start
      // Find the column end
      while( buf[pos++] != ',' ) ;
      _col++;                   // Found a column end
      _pos = pos;               // Set position update back in
      return cpos;
    }
  }

  // ------------------------------------------------------
  // ------------------------------------------------------
  // Ints not Strings
  
  // Skipping 8 characters at a time, searching for newlines!
  static final long ONES = 0x0101010101010101L;
  static final long NL_MASK = ONES * ('\n');
  static long has_zero(long x) {
    return (x-ONES) & ~x & 0x8080808080808080L;
  }
  private static final int ABASE  = UNSAFE.arrayBaseOffset(byte[].class);

  // This driver avoids all string allocation that happen per-Big-Data item. 
  static private NonBlockingHashMapLong<ProdData> driver6( int N ) throws IOException {
    NonBlockingHashMapLong<ProdData> prods = new NonBlockingHashMapLong<>();
    long src = str2long("ToClnt");
    long buy = str2long("Buy");
      
    // For each product, count records
    try( IntCSV csv = new IntCSV(file()); ) {
      int lines=N;
      for( int i=0; i<N; i++ ) {
        long src0 = csv.strcol(csv._idx_src);
        if( src0==0 ) { lines=i; break; } // No more data
        if( src0==src ) { // Filter on this row
          long b_s  = csv.strcol(csv._idx_bs    );
          int ord   = csv.intcol(csv._idx_ordqty);
          int wrk   = csv.intcol(csv._idx_wrkqty);
          int exc   = csv.intcol(csv._idx_excqty);
          long prod = csv.strcol(csv._idx_prod  );
          ProdData data = prods.get(prod);
          if( data == null )
            prods.put(prod, data = new ProdData(prod));
          data.cnt();                   // Count actions
          data.buy_sell(b_s==buy);      // Count buys and sells
          data.max_qty(ord,wrk,exc);    // Count quantities
        }
        if( !csv.skipToLineStart() ) { lines=i; break; } // Skip rest of prior line; align new line
      }
      
      // Add a bogus sentinel to return meta-data
      //ProdData data = new ProdData(",meta");
      //prods.put(str2long(",meta"),data);
      //data._cnt = lines;
      //data._tot_qty = csv._len;   // Total length read
      return prods;
    }
  }
    
  
  // ------------------------------------------------------
  static class IntCSV extends CSV implements AutoCloseable {
    FileInputStream _fis;
    int _pos, _lim, _eol;
    byte[] _buf;
    IntCSV( File f ) throws IOException {
      super(f);                 // Read and parse file header; 
      _len=0;                   // Remove counts already read by super()
      _br.close();              // Having read the headers, close the BR
      _fis=new FileInputStream(f);
      _buf = new byte[256*1024];
      fill();      
    }

    @Override public void close() throws IOException { _fis.close(); }
    
    // Fill the bytebuffer.  Guaranteed to be large enough to hold the largest
    // line (but maybe the line needs to be slid about to avoid a buffer crossing).
    boolean fill() throws IOException {
      // Copy partial line to buffer start
      System.arraycopy(_buf,_pos,_buf,0,_lim-_pos);
      _lim = _lim-_pos;
      _pos = 0;
      // Fill remaining buffer
      while( _lim < _buf.length ) {
        int len = _fis.read(_buf,_lim,_buf.length-_lim);
        if( len == -1 ) return _lim!=0;
        _lim += len;
        _len += len;
      }
      return true;
    }

    // Skip rest of line (guarenteed to be in the buffer).
    // Make sure the entire next line is in buffer.
    boolean skipToLineStart() throws IOException {
      _col=0;                   // Reset back to column 0
      int pos = _pos, lim=_lim; // Load pos,lim into a local variables
      byte[] buf = _buf;        // Same for buf
      if( _eol != 0 ) pos = _eol; // Have pre-computed line end; just skip ahead
      else
        // Find line end.  Guarenteed rest of line is in buffer; no buffer-end check
        while( buf[pos++] != '\n' ) ;
      _pos = pos;               // Set position update back in

      // Test to see if the NEW line is entirely in the buffer
      // Skip to aligned 8
      while( pos < lim && (pos&7)!=0 && buf[pos++] != '\n' ) ;
      // Load by size-8 chunks, and using SWAR (SIMD Words in A Register) find
      // the next `\n` char.
      while( pos+8 < lim && has_zero(NL_MASK ^ UNSAFE.getLong(buf,ABASE+pos))==0 )
        pos += 8;
      // Roll to the very next newline
      while( pos < lim && buf[pos++] != '\n' ) ;
      if( pos<lim ) { _eol = pos; return true; } // Have a line, record where it ends
      _eol = 0;                                  // Do not know where line ends
      return fill(); // Copy/compact partial line to start of buffer, and fill more into buffer
    }

    long strcol( int n ) throws IOException {
      int cpos = skip_col(n), pos = _pos-1;
      assert pos-cpos <= 7;
      long x=0;
      while( cpos < pos )
        x = (x<<8) | _buf[--pos];
      return x;
    }
    int intcol( int n ) throws IOException {
      int cpos = skip_col(n), pos = _pos-1;
      assert pos-cpos <= 7;
      int x=0;
      while( cpos < pos )
        x = x*10 + (_buf[cpos++]-'0');
      return x;
    }
    private int skip_col( int n ) throws IOException {
      assert _col <= n;
      int pos = _pos;           // Load pos into a local variable
      byte[] buf = _buf;        // Same for buf
      while( _col < n ) {
        while( buf[pos++] != ',' ) ;
        _col++;                 // Found a column end
      }
      int cpos = pos;           // Column start exclusive of ','
      // Find the column end
      while( buf[pos++] != ',' ) ;
      _col++;                   // Found a column end
      _pos = pos;               // Set position update back in
      return cpos;
    }
  }
  
  
  // ------------------------------------------------------
  // ------------------------------------------------------
  // Double-buffered file i/o

  // This driver avoids all string allocation that happen per-Big-Data item. 
  static private NonBlockingHashMapLong<ProdData> driver7( int N ) throws Exception {
    NonBlockingHashMapLong<ProdData> prods = new NonBlockingHashMapLong<>();
    long src = str2long("ToClnt");
    long buy = str2long("Buy");
      
    // For each product, count records
    try( DirectCSV csv = new DirectCSV(file()); ) {
      int lines=N;
      for( int i=0; i<N; i++ ) {
        long src0 = csv.strcol(csv._idx_src);
        if( src0==0 ) { lines=i; break; } // No more data
        if( src0==src ) { // Filter on this row
          long b_s  = csv.strcol(csv._idx_bs    );
          int ord   = csv.intcol(csv._idx_ordqty);
          int wrk   = csv.intcol(csv._idx_wrkqty);
          int exc   = csv.intcol(csv._idx_excqty);
          long prod = csv.strcol(csv._idx_prod  );
          ProdData data = prods.get(prod);
          if( data == null )
            prods.put(prod, data = new ProdData(prod));
          data.cnt();                   // Count actions
          data.buy_sell(b_s==buy);      // Count buys and sells
          data.max_qty(ord,wrk,exc);    // Count quantities
        }
        if( !csv.skipToLineStart() ) { lines=i; break; } // Skip rest of prior line; align new line
      }
      
      // Add a bogus sentinel to return meta-data
      //ProdData data = new ProdData(",meta");
      //prods.put(str2long(",meta"),data);
      //data._cnt = lines;
      //data._tot_qty = csv._len;   // Total length read
      return prods;
    }
  }

  // ------------------------------------------------------
  static class DirectCSV extends CSV implements AutoCloseable {
    static final int BUFSIZE = 16*1024;
    AsynchronousFileChannel _chan;
    long _filepos;
    int _pos, _lim, _eol;
    ByteBuffer _buf, _buf0, _buf1;
    Future<Integer> _future;    // The other pending buffer
    boolean _b;                 // false use _buf0, true use _buf1
    DirectCSV( File f ) throws Exception {
      super(f);                 // Read and parse file header; 
      _len=0;                   // Remove counts already read by super()
      _br.close();              // Having read the headers, close the BR
      _chan = AsynchronousFileChannel.open(file().toPath(),StandardOpenOption.READ);
      // Get a large misaligned DirectByteBuffer
      _buf = ByteBuffer.allocateDirect(3*BUFSIZE+1024);
      // Get 2 aligned chunks of size BUFSIZE, plus some slop on the end
      int align = _buf.alignmentOffset(0,BUFSIZE);
      _buf0 = _buf.slice(align        ,BUFSIZE);
      _buf1 = _buf.slice(align+BUFSIZE,BUFSIZE);
      // Start the first read
      _filepos = 0L;
      _future = _chan.read(_buf0,_filepos);
      _filepos += BUFSIZE;
      _b = !_b;
      fill();
    }
    
    @Override public void close() throws IOException { _chan.close(); }

    // Fill the bytebuffer.  Guaranteed to be large enough to hold the largest
    // line.  Partial bits from the current DBB have been copied out as needed,
    // since this call will flip to a new DBB, and wipe out the current one.
    ByteBuffer fill() throws Exception {
      Future<Integer> f = _future;  
      ByteBuffer buf_to_fill = _b ? _buf1 : _buf0;
      ByteBuffer buf_to_read = _b ? _buf0 : _buf1;
      buf_to_fill.clear();
      _future = _chan.read(buf_to_fill, _filepos );
      _filepos += BUFSIZE;      // Advance
      _b = !_b;                 // Flip active buffer
      int len = f.get();        // Block and get #loaded
      _len += len;
      buf_to_read.flip();
      return len == -1 ? null : buf_to_read; // -1 means EOF
    }
      
    // Skip rest of line
    boolean skipToLineStart() throws Exception {
      _col=0;                   // Reset back to column 0
      ByteBuffer buf = _b ? _buf1 : _buf0; // Get active buffer
      int pos = buf.position();
      int lim = buf.limit();
      while( true ) {
        char c = (char)buf.get(pos++);
        if( pos==lim ) {
          if( (buf=fill())==null )
            return false;               // Even if we got, return 0 on EOL
          pos = buf.position();
          lim = buf.limit();
        }
        if( c=='\n') {
          buf.position(pos);
          return true;
        }
      }
    }

    long strcol(int n) throws Exception {
      assert _col <= n;
      while( _col < n ) {
        if( !skipcol() ) return 0; // End of file
        _col++;
      }
      // Fill a long with string bytes
      long x=0, c;
      int cnt=0;
      while( (c=get())!=',' ) {
        x = (x>>8) | (c << 56); cnt++;
      }
      assert cnt <= 7;
      x >>= (8-cnt)*8;      
      _col++;
      return x;
    }
    boolean skipcol() throws Exception {      
      ByteBuffer buf = _b ? _buf1 : _buf0; // Get active buffer
      int pos = buf.position();
      int lim = buf.limit();
      while( true ) {
        char c = (char)buf.get(pos++);
        if( pos==lim ) {
          if( (buf=fill())==null )
            return false;               // Even if we got, return 0 on EOL
          pos = buf.position();
          lim = buf.limit();
        }
        if( c==',') {
          buf.position(pos);
          return true;
        }
      }
    }

    // Inline me please!
    char get() throws Exception {
      ByteBuffer buf = _b ? _buf1 : _buf0; // Get active buffer
      char c = (char)buf.get();
      if( !buf.hasRemaining() && (buf=fill())==null )
        return 0;               // Even if we got, return 0 on EOL
      return c;
    }    
    
    int intcol(int n) throws Exception {
      assert _col <= n;
      while( _col < n ) {
        if( !skipcol() ) return 0; // End of file
        _col++;
      }
      int x=0, c;
      while( (c=get())!=',' ) {
        x = x*10 + (c-'0');
      }
      _col++;
      return x;
    }
  }
  
  // ------------------------------------------------------
  
  // Reusable string-like class
  static class Str {
    byte[] _buf;
    int _off, _len;
    Str set( byte[] buf, int off, int len ) {
      _buf=buf; _off=off; _len=len;
      return this;
    }

    @Override public String toString() { return new String(_buf,_off,_len); }
    @Override public int hashCode() {
      int h = 0;
      for( int i=_off; i<_off+_len; i++ ) h = h*31+_buf[i];
      return h;
    }
    @Override public boolean equals( Object o ) {
      if( this==o ) return true;
      if( !(o instanceof Str) ) return false;
      Str s = (Str)o;
      if( _len != s._len ) return false;
      for( int i=0; i<_len; i++ )
        if( _buf[i+_off] != s._buf[i+s._off] )
          return false;
      return true;
    }
    boolean equals( String s ) {
      if( _len != s.length() ) return false;
      for( int i=0; i<_len; i++ )
        if( _buf[i+_off] != s.charAt(i) )
          return false;
      return true;
    }
    Str compact() {
      if( _off > 0 ) {
        _buf = Arrays.copyOfRange(_buf,_off,_off+_len);
        _off = 0;
      }
      return this;
    }
    
    int parseInt() {
      int i=0, sum=0;
      boolean neg = _buf[i+_off]=='-';
      if( neg ) i++;
      while( i<_len ) {
        int d = _buf[i++ +_off]-'0';
        if( 0 <= d && d <= 9 )
          sum = sum*10+d;
        else unimpl();
      }
      return neg ? -sum : sum;
    }
  }

  static long str2long( String s ) {
    int len = s.length();
    assert len <= 7;
    long x=0;
    for( int i=0; i<len; i++ )
      x = (x<<8)|s.charAt(len-1-i);
    return x;
  }
  static final StringBuilder SB = new StringBuilder();
  static String long2str( long x ) {
    int cnt=0;
    SB.setLength(0);
    while( (x&0xFF) != 0 && cnt++<8 ) {
      SB.append((char)(x&0xFF));
      x >>= 8;
    }
    return SB.toString();
  }

  private static RuntimeException unimpl() { return new RuntimeException("unimpl"); }
}
