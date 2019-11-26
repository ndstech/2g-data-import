package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

var (
	postgresConnect string
	dbName          string
	schemaName      string
	counterTable      string
	counterQualTable		string
	truncate        bool

	copyOptions    string
	splitCharacter string
	columns        string
	skipHeader     bool
	counterFileName    string
	counterQualFileName       string

	workers         int
	batchSize       int
	logBatches      bool
	reportingPeriod time.Duration
	verbose         bool

	columnCount int64
	rowCount    int64
	
	counterFile *os.File
	counterQualFile *os.File
	
	start time.Time
	end time.Time 
	took time.Duration
	rowsRead int64
	rowRate float64
	
	res string
	
	err error
)

type batch struct {
	rows []string
}

func check(e error) {
    if e != nil {
        panic(e)
    }
}

func init() {
	flag.StringVar(&postgresConnect, "connection", "host=localhost user=postgres sslmode=disable", "PostgreSQL connection url")
	flag.StringVar(&dbName, "db-name", "test", "Database where the destination table exists")
	flag.StringVar(&counterTable, "counter-table", "test_table", "Destination table for insertions")
	flag.StringVar(&counterQualTable, "counter-qual-table", "test_table", "Destination table for insertions")
	flag.StringVar(&schemaName, "schema", "public", "Desination table's schema")
	flag.BoolVar(&truncate, "truncate", false, "Truncate the destination table before insert")

	flag.StringVar(&copyOptions, "copy-options", "", "Additional options to pass to COPY (ex. NULL 'NULL')")
	flag.StringVar(&splitCharacter, "split", ",", "Character to split by")
	flag.StringVar(&columns, "columns", "", "Comma-separated columns present in CSV")
	flag.BoolVar(&skipHeader, "skip-header", false, "Skip the first line of the input")
	flag.StringVar(&counterFileName, "counter-file", "", "File to read from rather than stdin")
	flag.StringVar(&counterQualFileName, "counter-qual-file", "", "File to read from rather than stdin")

	flag.IntVar(&batchSize, "batch-size", 5000, "Number of rows per insert")
	flag.IntVar(&workers, "workers", 1, "Number of parallel requests to make")
	flag.BoolVar(&logBatches, "log-batches", false, "Whether to time individual batches.")
	flag.DurationVar(&reportingPeriod, "reporting-period", 0*time.Second, "Period to report insert stats; if 0s, intermediate results will not be reported")
	flag.BoolVar(&verbose, "verbose", false, "Print more information about copying statistics")

	flag.Parse()
}

func getConnectString() string {
	return fmt.Sprintf("%s dbname=%s", postgresConnect, dbName)
}

func getCounterTableName() string {
	return fmt.Sprintf("\"%s\".\"%s\"", schemaName, counterTable)
}

func getCounterQualTableName() string {
	return fmt.Sprintf("\"%s\".\"%s\"", schemaName, counterQualTable)
}

func main() {

	startMoving := time.Now()

	if truncate {
		dbBench := sqlx.MustConnect("postgres", getConnectString())
		
		fmt.Println("Truncating counter table")
		_, err = dbBench.Exec(fmt.Sprintf("TRUNCATE %s", getCounterTableName()))
		check(err)
		
		fmt.Println("Truncating counter qual table")
		_, err = dbBench.Exec(fmt.Sprintf("TRUNCATE %s", getCounterQualTableName()))
		check(err)

		err = dbBench.Close()
		check(err)
	}

	//BEGIN COUNTER TABLE
	fmt.Println("Start processing counter")
	var scanner *bufio.Scanner
	if len(counterFileName) > 0 {
		counterFile, err = os.Open(counterFileName)
		if err != nil {
			log.Fatal(err)
		}
		defer counterFile.Close()

		scanner = bufio.NewScanner(counterFile)
	} else {
		scanner = bufio.NewScanner(os.Stdin)
	}

	var wg sync.WaitGroup
	batchChan := make(chan *batch, workers)

	// Generate COPY workers
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go processBatches(&wg, batchChan)
	}

	// Reporting thread
	if reportingPeriod > (0 * time.Second) {
		go report()
	}

	start = time.Now()
	rowsRead = scan(batchSize, scanner, batchChan)
	close(batchChan)
	wg.Wait()
	end = time.Now()
	took = end.Sub(start)
	rowRate = float64(rowsRead) / float64(took.Seconds())

	res = fmt.Sprintf("COPY %d", rowsRead)
	if verbose {
		res += fmt.Sprintf(", took %v with %d worker(s) (mean rate %f/sec)", took, workers, rowRate)
	}
	fmt.Println(res)
	fmt.Println("Counter has been processed")
	//END COUNTER TABLE
	
	
	
	//BEGIN COUNTER QUAL TABLE
	fmt.Println("Start processing counter qual")
	var qualScanner *bufio.Scanner
	if len(counterQualFileName) > 0 {
		fmt.Println("Qual File Name : " + counterQualFileName)
		counterQualFile, err = os.Open(counterQualFileName)
		if err != nil {
			log.Fatal(err)
		}
		defer counterQualFile.Close()

		qualScanner = bufio.NewScanner(counterQualFile)
	} else {
		qualScanner = bufio.NewScanner(os.Stdin)
	}

	var qualWg sync.WaitGroup
	qualBatchChan := make(chan *batch, workers)

	// Generate COPY workers
	for i := 0; i < workers; i++ {
		qualWg.Add(1)
		go processQual(&qualWg, qualBatchChan)
	}

	start = time.Now()
	rowsRead = scan(batchSize, qualScanner, qualBatchChan)
	close(qualBatchChan)
	qualWg.Wait()
	end = time.Now()
	took = end.Sub(start)
	rowRate = float64(rowsRead) / float64(took.Seconds())

	res = fmt.Sprintf("COPY %d", rowsRead)
	if verbose {
		res += fmt.Sprintf(", took %v with %d worker(s) (mean rate %f/sec)", took, workers, rowRate)
	}
	fmt.Println(res)
	fmt.Println("Counter qual has been processed")
	//END COUNTER QUAL TABLE
	
	
	
	dbBench := sqlx.MustConnect("postgres", getConnectString())
	defer dbBench.Close()
	
	fmt.Println("Insert data : counter_2g_lastday")
	dbBench.MustExec(`insert into counter_2g_lastday
	select a.resulttime, a.unique_id, a.mbsc, a.cellname, a.cellindex, a.lac, a.ci,
	a.rr307, a.rr300, a.ar3010a, a.ar3010b, a.ar3017a, a.ar3017b, a.ar3018a, a.ar3018b, a.ar3020a, a.ar3020b, a.ar3027a, a.ar3027b, a.ar3028a, a.ar3028b, a.ch303, a.ch301, a.ch300, a.m3128, a.m3128a, a.m3030a, a.m3030b, a.m3030c, a.m3030d, a.m3030e, a.m3030f, a.m3030g, a.m3030h, a.m3030i, a.m3030j, a.m3030k, a.a330, a.a331, a.a337, a.a339, a.k3021, a.k3011a, a.k3011b, a.k3020, a.k3010a, a.k3010b, a.ch313, a.ch333, a.ch311, a.ch331, a.k3003, a.cm30, a.k3000, a.cm33, a.m315, a.m325, a.k3013a, a.ch323, a.ch343, a.k3014, a.ca303j, a.ca300j, a.ca313, a.ca310, a.k3001, a.k3004, a.ch321, a.k3005, a.k3006, a.k3015, a.k3016, a.r373, a.ch341, a.ca3030j, a.k3034, a.ar9303, a.k3023, a.cm3300, a.cm3301, a.cm3302, a.cm330, a.cm331, a.cm332, a.cm333, a.cm334, a.cm335, a.m305, a.m345, a.m355, a.ch342, a.h3429a, a.th343, a.ch340, a.ch322, a.h3229a, a.th323, a.ch320, a.k3040, a.ch330, a.ch310, a.l9302, a.l9303, a.l9304, a.l9305, a.l9306, a.l9307, a.l9308, a.l9309, a.l9310, a.a9002, a.a9001, a.a9102, a.a9101, a.a9202, a.a9201, a.a9302, a.a9301, a.l9201, a.l9202, a.l9203, a.l9204, a.l9205, a.l9206, a.l9207, a.l9208, a.l9209, a.l9001, a.l9002, a.l9003, a.l9004, a.l9005, a.l9006, a.l9007, a.l9008, a.l9009, a.l9102, a.l9103, a.l9104, a.l9105, a.l9210, a.a9006, a.a9007, a.a9206, a.a9207, a.a9106, a.a9306, a.r9204, a.r9205, a.r9206, a.r9207, a.a9003, a.a9005, a.a9103, a.a9105, a.a9203, a.a9303, a.a9305, a.a9205, a.a9501, a.a9507, a.tl9014, a.tl9114, a.tl9232, a.tl9333, a.a9104, a.a9304, a.a9315, a.a9115, a.a9210, a.a9010, a.a9109, a.a9309, a.a9209, a.a9009, a.a9108, a.a9308, a.a9307, a.a9107, a.a9008, a.a9208, a.l9211, a.l9212, a.l9213, a.l9214, a.l9215, a.l9216, a.l9217, a.l9218, a.l9219, a.l9311, a.l9312, a.l9313, a.l9314, a.l9315, a.l9316, a.l9317, a.l9318, a.l9319, a.l9106, a.l9107, a.l9108, a.l9109, a.tl9338, a.tl9123, a.tl9237, a.tl9023, a.a9318, a.a9118, a.a9004, a.cm30a, a.cm30c, a.cm30d, a.m3000a, a.m3000b, a.m3000c, a.m3001a, a.m3001b, a.m3001c, a.m3001d, a.m3001e, a.m3002, a.m302, a.m303, a.m304, a.m3100a, a.m3100b, a.m3100c, a.m3101a, a.m3101b, a.m3101c, a.m3101d, a.m3101e, a.m3102, a.m312, a.m313, a.m314, a.m3200a, a.m3200b, a.m3200c, a.m3201a, a.m3201b, a.m3201c, a.m3201d, a.m3201e, a.m3202, a.m322, a.m323, a.m324, a.a300a, a.a300c, a.a300d, a.a300e, a.a300f, a.a300k, a.a3010, a.a3030a, a.a3030b, a.a3030c, a.a3030d, a.a3030e, a.a3030f, a.a3030g, a.a3030h, a.a3030i, a.a3030j, a.a3030k, a.a3039j, a.a3040, a.a9204, a.a9237, a.a9238, a.a9335, a.a9336, a.h3027ca, a.h3028ca, a.h3127ca, a.h3128ca, a.h3327ca, a.h3328ca, a.h3429ca, a.k3013b, a.r3100a, a.r9101, a.r9109, a.r9110, a.r9111, a.r9112, a.r9115, a.cm3303a, a.a9216, a.a9016, a.a9341, a.a9343, a.payload_gprs_mbit, a.payload_edge_mbit, a.payload_gprs_mbyte, a.payload_edge_mbyte, b.cr443a, b.cr443b, b.r4419a, b.r4419b, b.cs410a, b.cs410b, b.cs410c, b.cs410d, b.cs411a, b.cs411b, b.cs411c, b.cs411d, b.cs412a, b.cs412b, b.cs412c, b.cs412d, b.cs413a, b.cs413b, b.cs413c, b.cs413d, b.cs414a, b.cs414b, b.cs414c, b.cs414d, b.cs415a, b.cs415b, b.cs415c, b.cs415d, b.cs416a, b.cs416b, b.cs416c, b.cs416d, b.cs417a, b.cs417b, b.cs417c, b.cs417d, b.as4207a, b.as4207b, b.as4207c, b.as4207d, b.as4207e, b.as4208a, b.as4208b, b.as4208c, b.as4208d, b.as4208e, b.s4556, b.s4557, b.s4400a, b.s4401a, b.s4402a, b.s4403a, b.s4404a, b.s4405a, b.s4406a, b.s4407a, b.s4408a, b.s4409a, b.s4410a, b.s4411a, b.s4412a, b.s4413a, b.s4414a, b.s4415a, b.s4416a, b.s4417a, b.s4418a, b.s4419a, b.s4420a, b.s4421a, b.s4422a, b.s4423a, b.s4424a, b.s4425a, b.s4426a, b.s4427a, b.s4428a, b.s4429a, b.s4430a, b.s4432a, b.s4434a, b.s4436a, b.s4438a, b.s4440a, b.s4445a, b.s4450a, b.s4455a, b.s4463a
	from counter_2g_temp a
	inner join counter_2g_qual_temp b on a.unique_id=b.unique_id and a.resulttime=b.resulttime
	on conflict do nothing;`)
	
	fmt.Println("Insert data : counter_2g_hourly")
	dbBench.MustExec("insert into counter_2g_hourly select * from counter_2g_lastday on conflict do nothing;")
	
	fmt.Println("Insert data : counter_2g_daily")
	dbBench.MustExec(`insert into counter_2g_daily 
	select time_bucket('1 day',resulttime) date, unique_id, mbsc, cellname, cellindex, lac, ci,
	sum(RR307), sum(RR300), sum(AR3010A), sum(AR3010B), sum(AR3017A), sum(AR3017B), sum(AR3018A), sum(AR3018B), sum(AR3020A), sum(AR3020B), sum(AR3027A), sum(AR3027B), sum(AR3028A), sum(AR3028B), sum(CH303), sum(CH301), sum(CH300), sum(M3128), sum(M3128A), sum(M3030A), sum(M3030B), sum(M3030C), sum(M3030D), sum(M3030E), sum(M3030F), sum(M3030G), sum(M3030H), sum(M3030I), sum(M3030J), sum(M3030K), sum(A330), sum(A331), sum(A337), sum(A339), sum(K3021), sum(K3011A), sum(K3011B), sum(K3020), sum(K3010A), sum(K3010B), sum(CH313), sum(CH333), sum(CH311), sum(CH331), sum(K3003), sum(CM30), sum(K3000), sum(CM33), sum(M315), sum(M325), sum(K3013A), sum(CH323), sum(CH343), sum(K3014), sum(CA303J), sum(CA300J), sum(CA313), sum(CA310), sum(K3001), sum(K3004), sum(CH321), sum(K3005), sum(K3006), sum(K3015), sum(K3016), sum(R373), sum(CH341), sum(CA3030J), sum(K3034), sum(AR9303), sum(K3023), sum(CM3300), sum(CM3301), sum(CM3302), sum(CM330), sum(CM331), sum(CM332), sum(CM333), sum(CM334), sum(CM335), sum(M305), sum(M345), sum(M355), sum(CH342), sum(H3429A), sum(TH343), sum(CH340), sum(CH322), sum(H3229A), sum(TH323), sum(CH320), sum(K3040), sum(CH330), sum(CH310), sum(L9302), sum(L9303), sum(L9304), sum(L9305), sum(L9306), sum(L9307), sum(L9308), sum(L9309), sum(L9310), sum(A9002), sum(A9001), sum(A9102), sum(A9101), sum(A9202), sum(A9201), sum(A9302), sum(A9301), sum(L9201), sum(L9202), sum(L9203), sum(L9204), sum(L9205), sum(L9206), sum(L9207), sum(L9208), sum(L9209), sum(L9001), sum(L9002), sum(L9003), sum(L9004), sum(L9005), sum(L9006), sum(L9007), sum(L9008), sum(L9009), sum(L9102), sum(L9103), sum(L9104), sum(L9105), sum(L9210), sum(A9006), sum(A9007), sum(A9206), sum(A9207), sum(A9106), sum(A9306), sum(R9204), sum(R9205), sum(R9206), sum(R9207), sum(A9003), sum(A9005), sum(A9103), sum(A9105), sum(A9203), sum(A9303), sum(A9305), sum(A9205), sum(A9501), sum(A9507), sum(TL9014), sum(TL9114), sum(TL9232), sum(TL9333), sum(A9104), sum(A9304), sum(A9315), sum(A9115), sum(A9210), sum(A9010), sum(A9109), sum(A9309), sum(A9209), sum(A9009), sum(A9108), sum(A9308), sum(A9307), sum(A9107), sum(A9008), sum(A9208), sum(L9211), sum(L9212), sum(L9213), sum(L9214), sum(L9215), sum(L9216), sum(L9217), sum(L9218), sum(L9219), sum(L9311), sum(L9312), sum(L9313), sum(L9314), sum(L9315), sum(L9316), sum(L9317), sum(L9318), sum(L9319), sum(L9106), sum(L9107), sum(L9108), sum(L9109), sum(TL9338), sum(TL9123), sum(TL9237), sum(TL9023), sum(A9318), sum(A9118), sum(A9004), sum(CM30A), sum(CM30C), sum(CM30D), sum(M3000A), sum(M3000B), sum(M3000C), sum(M3001A), sum(M3001B), sum(M3001C), sum(M3001D), sum(M3001E), sum(M3002), sum(M302), sum(M303), sum(M304), sum(M3100A), sum(M3100B), sum(M3100C), sum(M3101A), sum(M3101B), sum(M3101C), sum(M3101D), sum(M3101E), sum(M3102), sum(M312), sum(M313), sum(M314), sum(M3200A), sum(M3200B), sum(M3200C), sum(M3201A), sum(M3201B), sum(M3201C), sum(M3201D), sum(M3201E), sum(M3202), sum(M322), sum(M323), sum(M324), sum(A300A), sum(A300C), sum(A300D), sum(A300E), sum(A300F), sum(A300K), sum(A3010), sum(A3030A), sum(A3030B), sum(A3030C), sum(A3030D), sum(A3030E), sum(A3030F), sum(A3030G), sum(A3030H), sum(A3030I), sum(A3030J), sum(A3030K), sum(A3039J), sum(A3040), sum(A9204), sum(A9237), sum(A9238), sum(A9335), sum(A9336), sum(H3027Ca), sum(H3028Ca), sum(H3127Ca), sum(H3128Ca), sum(H3327Ca), sum(H3328Ca), sum(H3429Ca), sum(K3013B), sum(R3100A), sum(R9101), sum(R9109), sum(R9110), sum(R9111), sum(R9112), sum(R9115), sum(CM3303A), sum(A9216), sum(A9016), sum(A9341), sum(A9343), sum(Payload_GPRS_Mbit), sum(Payload_EDGE_Mbit), sum(Payload_GPRS_Mbyte), sum(Payload_EDGE_Mbyte), sum(CR443A), sum(CR443B), sum(R4419A), sum(R4419B), sum(CS410A), sum(CS410B), sum(CS410C), sum(CS410D), sum(CS411A), sum(CS411B), sum(CS411C), sum(CS411D), sum(CS412A), sum(CS412B), sum(CS412C), sum(CS412D), sum(CS413A), sum(CS413B), sum(CS413C), sum(CS413D), sum(CS414A), sum(CS414B), sum(CS414C), sum(CS414D), sum(CS415A), sum(CS415B), sum(CS415C), sum(CS415D), sum(CS416A), sum(CS416B), sum(CS416C), sum(CS416D), sum(CS417A), sum(CS417B), sum(CS417C), sum(CS417D), sum(AS4207A), sum(AS4207B), sum(AS4207C), sum(AS4207D), sum(AS4207E), sum(AS4208A), sum(AS4208B), sum(AS4208C), sum(AS4208D), sum(AS4208E), sum(S4556), sum(S4557), sum(S4400A), sum(S4401A), sum(S4402A), sum(S4403A), sum(S4404A), sum(S4405A), sum(S4406A), sum(S4407A), sum(S4408A), sum(S4409A), sum(S4410A), sum(S4411A), sum(S4412A), sum(S4413A), sum(S4414A), sum(S4415A), sum(S4416A), sum(S4417A), sum(S4418A), sum(S4419A), sum(S4420A), sum(S4421A), sum(S4422A), sum(S4423A), sum(S4424A), sum(S4425A), sum(S4426A), sum(S4427A), sum(S4428A), sum(S4429A), sum(S4430A), sum(S4432A), sum(S4434A), sum(S4436A), sum(S4438A), sum(S4440A), sum(S4445A), sum(S4450A), sum(S4455A), sum(S4463A)
	from counter_2g_lastday group by date, unique_id, mbsc, cellname, cellindex, lac, ci
	on conflict do nothing;`)
	
	fmt.Println("Truncate data : counter_2g_qual_temp")
	dbBench.MustExec("TRUNCATE counter_2g_qual_temp;")
	
	fmt.Println("Truncate data : counter_2g_temp")
	dbBench.MustExec("TRUNCATE counter_2g_temp;")
	
	fmt.Println("Truncate data : counter_2g_lastday")
	dbBench.MustExec("TRUNCATE counter_2g_lastday;")

	endMoving := time.Now()
	movingDuration := endMoving.Sub(startMoving)
	fmt.Println(fmt.Sprintf("All operations has been completed in %v seconds)", movingDuration))
}

func report() {
	start := time.Now()
	prevTime := start
	prevRowCount := int64(0)

	for now := range time.NewTicker(reportingPeriod).C {
		rCount := atomic.LoadInt64(&rowCount)

		took := now.Sub(prevTime)
		rowrate := float64(rCount-prevRowCount) / float64(took.Seconds())
		overallRowrate := float64(rCount) / float64(now.Sub(start).Seconds())
		totalTook := now.Sub(start)

		fmt.Printf("at %v, row rate %f/sec (period), row rate %f/sec (overall), %E total rows\n", totalTook-(totalTook%time.Second), rowrate, overallRowrate, float64(rCount))

		prevRowCount = rCount
		prevTime = now
	}

}

func scan(itemsPerBatch int, scanner *bufio.Scanner, batchChan chan *batch) int64 {
	rows := make([]string, 0, itemsPerBatch)
	var linesRead int64
	
	if skipHeader {
		if verbose {
			fmt.Println("Skipping the first line of the input.")
		}
		scanner.Scan()
	}

	for scanner.Scan() {
		linesRead++

		rows = append(rows, scanner.Text())
		if len(rows) >= itemsPerBatch { // dispatch to COPY worker & reset
			batchChan <- &batch{rows}
			rows = make([]string, 0, itemsPerBatch)
		}
	}

	if err = scanner.Err(); err != nil {
		log.Fatalf("Error reading input: %s", err.Error())
	}

	if len(rows) > 0 {
		batchChan <- &batch{rows}
	}

	return linesRead
}

func processBatches(wg *sync.WaitGroup, C chan *batch) {
	dbBench := sqlx.MustConnect("postgres", getConnectString())
	defer dbBench.Close()
	columnCountWorker := int64(0)
	for batch := range C {
		start := time.Now()

		tx := dbBench.MustBegin()
		delimStr := fmt.Sprintf("'%s'", splitCharacter)
		if splitCharacter == "\\t" {
			delimStr = "E" + delimStr
		}
		var copyCmd string
		if columns != "" {
			copyCmd = fmt.Sprintf("COPY %s(%s) FROM STDIN WITH DELIMITER %s %s", getCounterTableName(), columns, delimStr, copyOptions)
		} else {
			copyCmd = fmt.Sprintf("COPY %s FROM STDIN WITH DELIMITER %s %s", getCounterTableName(), delimStr, copyOptions)
		}

		stmt, err := tx.Prepare(copyCmd)
		if err != nil {
			panic(err)
		}

		sChar := splitCharacter
		if sChar == "\\t" {
			sChar = "\t"
		}
		
		for _, line := range batch.rows {
			sp := strings.Split(line, sChar)
			
			//fmt.Printf("%v \n\n", sp)
			//fmt.Println(sp[0])

			var unique_id = sp[4] + sp[5]
			slice_1 := make([]string, 2)
			slice_1[0] = sp[0]
			slice_1[1] = unique_id

			var slice_2 []string = sp[1:]
			new_sp := append(slice_1, slice_2...)
			
			finalCommand := strings.Join(new_sp, ",")

			columnCountWorker += int64(len(new_sp))
			// For some reason this is only needed for tab splitting
			if sChar == "\t" {
				args := make([]interface{}, len(new_sp))
				for i, v := range new_sp {
					args[i] = v
				}
				_, err = stmt.Exec(args...)
			} else {
				//fmt.Println("Masuk else")
				_, err = stmt.Exec(finalCommand)
			}

			if err != nil {
				panic(err)
			}
		}
		atomic.AddInt64(&columnCount, columnCountWorker)
		atomic.AddInt64(&rowCount, int64(len(batch.rows)))
		columnCountWorker = 0

		err = stmt.Close()
		if err != nil {
			panic(err)
		}

		err = tx.Commit()
		if err != nil {
			panic(err)
		}

		if logBatches {
			took := time.Now().Sub(start)
			fmt.Printf("[BATCH] took %v, batch size %d, row rate %f/sec\n", took, batchSize, float64(batchSize)/float64(took.Seconds()))
		}

	}
	wg.Done()
}

// processBatches reads batches from C and writes them to the target server, while tracking stats on the write.
func processQual(wg *sync.WaitGroup, C chan *batch) {
	dbBench := sqlx.MustConnect("postgres", getConnectString())
	defer dbBench.Close()
	columnCountWorker := int64(0)
	for batch := range C {
		start := time.Now()

		tx := dbBench.MustBegin()
		delimStr := fmt.Sprintf("'%s'", splitCharacter)
		if splitCharacter == "\\t" {
			delimStr = "E" + delimStr
		}
		var copyCmd string
		if columns != "" {
			copyCmd = fmt.Sprintf("COPY %s(%s) FROM STDIN WITH DELIMITER %s %s", getCounterQualTableName(), columns, delimStr, copyOptions)
		} else {
			copyCmd = fmt.Sprintf("COPY %s FROM STDIN WITH DELIMITER %s %s", getCounterQualTableName(), delimStr, copyOptions)
		}
		
		//fmt.Println(copyCmd)

		stmt, err := tx.Prepare(copyCmd)
		if err != nil {
			panic(err)
		}

		// Need to cover the string-ified version of the character to actual character for correct split
		sChar := splitCharacter
		if sChar == "\\t" {
			sChar = "\t"
		}
		
		row := 0
		for _, line := range batch.rows {
			row = row + 1
			sp := strings.Split(line, sChar)

			var unique_id = sp[4] + sp[5]
			slice_1 := make([]string, 2)
			slice_1[0] = sp[0]
			slice_1[1] = unique_id

			var slice_2 []string = sp[1:]
			new_sp := append(slice_1, slice_2...)
			
			finalCommand := strings.Join(new_sp, ",")
			//fmt.Println(fmt.Sprintf("Column length : %d", len(sp)))
			//fmt.Println(finalCommand)

			columnCountWorker += int64(len(new_sp))
			// For some reason this is only needed for tab splitting
			if sChar == "\t" {
				args := make([]interface{}, len(new_sp))
				for i, v := range new_sp {
					args[i] = v
				}
				_, err = stmt.Exec(args...)
			} else {
				//fmt.Println("Masuk else")
				_, err = stmt.Exec(finalCommand)
			}

			if err != nil {
				fmt.Println(fmt.Sprintf("Line : %d", row))
				fmt.Println(finalCommand)
				panic(err)
			}
		}
		atomic.AddInt64(&columnCount, columnCountWorker)
		atomic.AddInt64(&rowCount, int64(len(batch.rows)))
		columnCountWorker = 0

		err = stmt.Close()
		if err != nil {
			panic(err)
		}

		err = tx.Commit()
		if err != nil {
			panic(err)
		}

		if logBatches {
			took := time.Now().Sub(start)
			fmt.Printf("[BATCH] took %v, batch size %d, row rate %f/sec\n", took, batchSize, float64(batchSize)/float64(took.Seconds()))
		}

	}
	wg.Done()
}
