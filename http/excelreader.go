package http

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/xuri/excelize/v2"
)

const (
	chunkVolume = 1000    // Size of slice of ohcl to save to the db as batches
	MB          = 1 << 20 // File size factor for worker pool
)

// Object holding csv rows in chunks and channel flow for saving the chunks in batches
type ProcessPool struct {
	chunk             []string
	dataChan          chan []string
	wg                *sync.WaitGroup
	numWorkers        int //Number of worker to process db ops
	errorMessage      string
	excelLinesRead    int  // Total number of lines read
	totalChunkFetched int  // Total number of row chunks fetched
	done              bool // Checks if every lines has been read
	mutex             sync.Mutex
}

// generateExcelChunk reads chunks of rows from a small Excel file
func (processPool *ProcessPool) generateExcelChunkSV(f *excelize.File, sheetName string) {
	rows, err := f.GetRows(sheetName)

	rows = rows[1:]
	if err != nil {
		processPool.errorMessage = err.Error()
		close(processPool.dataChan) // close data channel
		return
	}

	totalRows := len(rows)
	totalChunks := (totalRows + chunkVolume - 1) / chunkVolume

	wg := sync.WaitGroup{}
	wg.Add(totalChunks)
	for i := 0; i < totalRows; i += chunkVolume {
		startIdx := i
		endIdx := i + chunkVolume
		if endIdx > totalRows {
			endIdx = totalRows
		}

		go func(start, end int, wg *sync.WaitGroup) {
			defer wg.Done()
			chunk := rows[start:end]

			// Send chunk to data channel
			processPool.dataChan <- getChunkIDs(chunk)
			processPool.excelLinesRead += endIdx - startIdx
		}(startIdx, endIdx, &wg)
	}

	wg.Wait()
	close(processPool.dataChan)
}

func getChunkIDs(chunk [][]string) []string {
	ids := make([]string, 0, len(chunk))
	for _, cols := range chunk {
		id := cols[len(cols)-1]
		ids = append(ids, id)
	}

	return ids
}

// generateExcelChunk reads chunks of rows from a large Excel file
func (processPool *ProcessPool) generateExcelChunkLV(rows *excelize.Rows) {

	chunk := make([]string, 0, chunkVolume)

	wg := sync.WaitGroup{}
	mtx := sync.RWMutex{}

	wg.Add(processPool.numWorkers)
	for i := 0; i < processPool.numWorkers; i++ {
		go func(mtx *sync.RWMutex, wg *sync.WaitGroup) {
			defer wg.Done()
			mtx.Lock()
			for rows.Next() {
				cols, err := rows.Columns()
				if err != nil {
					processPool.errorMessage = err.Error()
					close(processPool.dataChan) // close data channel
					mtx.Unlock()
					return
				}
				id := cols[len(cols)-1]

				chunk = append(chunk, id)

				if len(chunk) == chunkVolume {
					processPool.dataChan <- chunk
					processPool.excelLinesRead += chunkVolume
					chunk = make([]string, 0)
				}
			}
			mtx.Unlock()
		}(&mtx, &wg)

	}

	wg.Wait()
	// Check for reminder in the chunk
	if len(chunk) > 0 {
		processPool.dataChan <- chunk
		processPool.excelLinesRead += len(chunk)
	}

	close(processPool.dataChan)
}

// processExcelChunk concurrently processes chunks of rows from the data channel
func (processPool *ProcessPool) processExcelChunk() {
	for i := 0; i < processPool.numWorkers; i++ {
		go func() {
			defer processPool.wg.Done()
			for rows := range processPool.dataChan {
				// Process each chunk here, such as inserting into database

				// Check if all the Excel lines have been saved
				processPool.mutex.Lock()
				processPool.totalChunkFetched += len(rows)
				if processPool.excelLinesRead == processPool.totalChunkFetched {
					processPool.done = true
					processPool.mutex.Unlock()
					return // All Excel lines are saved
				}
				processPool.mutex.Unlock()
			}
		}()
	}
}

func ReadExcel(ctx *gin.Context) {

	// Increase the context timeout in case of a very large Excel file
	_, cancel := context.WithTimeout(ctx.Request.Context(), 3*time.Minute)
	defer cancel()

	file, err := ctx.FormFile("file")
	if err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"error": err.Error()})
		return
	}

	if strings.ToLower(filepath.Ext(file.Filename)) != ".xlsx" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Expected an XLSX file"})
		return
	}

	fileContent, err := file.Open()
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	defer fileContent.Close()

	// Open XLSX file using excelize
	xclF, err := excelize.OpenReader(fileContent)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	defer xclF.Close()

	// Get the first sheet name
	sheetName := xclF.GetSheetName(0)

	//iteration approach for rows
	iteRows, err := xclF.Rows(sheetName)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// Read and get header:
	// Read first row (header row) to allow reader of data from real data role in the generateExcelChunkLV
	iteRows.Next()
	_, err = iteRows.Columns()
	// Use header for more dynmic purpose e.g particular column process
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	fileVolume := file.Size / MB
	numWorkers := int(math.Min(math.Max(float64(runtime.NumCPU()-2), 20), float64(fileVolume+20)))

	var wg sync.WaitGroup
	wg.Add(numWorkers)

	processPool := ProcessPool{
		wg:         &wg,
		chunk:      make([]string, 0, chunkVolume),
		dataChan:   make(chan []string, numWorkers),
		numWorkers: numWorkers,
	}

	go processPool.processExcelChunk() // Use worker pool to process excel in chunks
	if fileVolume < 1 {
		processPool.generateExcelChunkSV(xclF, sheetName) // Read excel rows into chunksv
	} else {
		processPool.generateExcelChunkLV(iteRows) // Read excel rows into chunks
	}

	// Lock flow until all workers are done
	wg.Wait()

	ctx.JSON(http.StatusOK, gin.H{
		"readline":      processPool.excelLinesRead,
		"processedline": processPool.totalChunkFetched,
		"iscompleted":   processPool.done,
	})

}

func PopulateExcel(ctx *gin.Context) {

	dummy := excelize.NewFile()

	shtName := dummy.GetSheetName(0)

	dummy.SetCellValue(shtName, "A1", "REQ_ID")

	for i, req_id := range ids {
		dummy.SetCellValue(shtName, fmt.Sprintf("A%d", i+2), req_id)
	}

	dummyBuffer, err := dummy.WriteToBuffer()
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	contentType := "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
	contentLength := dummyBuffer.Len()
	extraHeaders := map[string]string{
		"Content-Disposition": `attachment; filename="ids.xlsx"`, // Set the filename for download
	}
	ctx.DataFromReader(200, int64(contentLength), contentType, dummyBuffer, extraHeaders)

}

var ids = []string{
	"IN5904004437282",
	"IN7210004437258",
	"IN3172004437217",
	"IN0547004437187",
	"IN2406004437177",
	"IN6175004437167",
	"IN4143004437127",
	"IN9674004437095",
	"IN3329004437043",
	"IN4999004437003",
	"IN7361004436943",
	"IN2968004436914",
	"IN4650004436885",
	"IN2616004436847",
	"IN9605004436826",
	"IN8974004436801",
	"IN2845004436787",
	"IN5516004436764",
	"IN0299004436737",
	"IN5735004436711",
	"IN3254004436693",
	"IN3251004436680",
	"IN8638004436577",
	"IN4658004436548",
	"IN5782004436481",
	"IN5931004436467",
	"IN8297004436442",
	"IN8374004436408",
	"IN8458004436358",
	"IN0414004436331",
	"IN9056004436300",
	"IN1804004436279",
	"IN0381004436253",
	"IN0799004436241",
	"IN5930004436206",
	"IN7805004436177",
	"IN8744004436156",
	"IN2753004436118",
	"IN1665004436107",
	"IN0862004436081",
	"IN2959004436045",
	"IN5623004435994",
	"IN1583004435951",
	"IN7162004435917",
	"IN1123004435791",
	"IN7376004435745",
	"IN9972004435730",
	"IN2570004435713",
	"IN3388004435677",
	"IN3434004435666",
	"IN1287004435623",
	"IN0796004435597",
	"IN4219004435552",
	"IN2337004435528",
	"IN2073004435500",
	"IN8478004435482",
	"IN3386004435464",
	"IN9639004435440",
	"IN1225004435418",
	"IN9244004435401",
	"IN0730004435352",
	"IN5240004435334",
	"IN1808004435312",
	"IN3139004435285",
	"IN4585004435274",
	"IN8373004435237",
	"IN0367004435207",
	"IN6968004435184",
	"IN3464004435147",
	"IN7614004435120",
	"IN2757004435108",
	"IN3498004435080",
	"IN7016004435046",
	"IN8014004435029",
	"IN2510004434958",
	"IN6149004434941",
	"IN4121004434925",
	"IN7141004434917",
	"IN7040004434894",
	"IN8604004434872",
	"IN8086004434846",
	"IN1799004434833",
	"IN6059004434750",
	"IN6340004434712",
	"IN5249004434674",
	"IN4592004434668",
	"IN5824004434660",
	"IN7546004434641",
	"IN0967004434583",
	"IN2491004434554",
	"IN6793004434531",
	"IN4302004434491",
	"IN8102004434455",
	"IN1540004434425",
	"IN2511004434405",
	"IN1468004434358",
	"IN9889004434255",
	"IN2348004434228",
	"IN3339004434211",
	"IN4073004434145",
	"IN6916004434093",
	"IN3157004434066",
	"IN2855004434038",
	"IN1212004434008",
	"IN4272004433986",
	"IN6601004433949",
	"IN2409004433911",
	"IN8827004433819",
	"IN0073004433784",
	"IN8072004433756",
	"IN8483004433728",
	"IN7670004433656",
	"IN6047004433637",
	"IN8136004433604",
	"IN7159004433574",
	"IN0947004433545",
	"IN3499004433521",
	"IN8139004433482",
	"IN0142004433421",
	"IN7745004433383",
	"IN5672004433370",
	"IN5991004433346",
	"IN4006004433317",
	"IN4600004433279",
	"IN7420004433269",
	"IN4591004433239",
	"IN9107004433205",
	"IN9890004433176",
	"IN8167004433154",
	"IN9866004433135",
	"IN3535004433100",
	"IN0565004433062",
	"IN2222004433049",
	"IN3426004433039",
	"IN1329004433018",
	"IN8743004432986",
	"IN3668004432964",
	"IN6958004432938",
	"IN0851004432880",
	"IN1601004432860",
	"IN9519004432851",
	"IN8717004432823",
	"IN5368004432795",
	"IN5728004432752",
	"IN0863004432737",
	"IN8957004432705",
	"IN6790004432694",
	"IN1303004432680",
	"IN3976004432660",
	"IN1121004432634",
	"IN2908004432612",
	"IN9851004432591",
	"IN8264004432577",
	"IN4360004432564",
	"IN3562004432516",
	"IN0573004432471",
	"IN6624004432448",
	"IN7992004432400",
	"IN9481004432288",
	"IN0190004432241",
	"IN5794004432198",
	"IN9361004432172",
	"IN1648004432132",
	"IN5271004432090",
	"IN5028004432048",
	"IN7712004431984",
	"IN6897004431956",
	"IN5129004431913",
	"IN5497004431878",
	"IN2524004431843",
	"IN2555004431814",
	"IN8965004431799",
	"IN1778004431792",
	"IN1991004431763",
	"IN3555004431721",
	"IN0124004431698",
	"IN8339004431687",
	"IN5406004431673",
	"IN3348004431654",
	"IN7016004431607",
	"IN4585004431564",
	"IN7212004431544",
	"IN6654004431521",
	"IN2550004431501",
	"IN7399004431468",
	"IN7977004431442",
	"IN0634004431425",
	"IN5763004431382",
	"IN2865004431364",
	"IN1122004431320",
	"IN4209004431292",
	"IN2165004431269",
	"IN3867004431255",
	"IN4129004431226",
	"IN8000004431189",
	"IN6173004431173",
	"IN6836004431145",
	"IN5451004431124",
	"IN8930004431079",
	"IN8172004431048",
	"IN3703004431033",
	"IN3736004431017",
	"IN1433004430981",
	"IN4890004430951",
	"IN2619004430897",
	"IN5751004430875",
	"IN4475004430854",
	"IN6726004430839",
	"IN3987004430782",
	"IN6077004430752",
	"IN9906004430716",
	"IN9773004430690",
	"IN7125004430650",
	"IN6626004430617",
	"IN7914004430608",
	"IN0445004430566",
	"IN0683004430459",
	"IN7283004430443",
	"IN1700004430389",
	"IN3566004430346",
	"IN2791004430317",
	"IN1455004430278",
	"IN5184004430229",
	"IN1454004430196",
	"IN3301004430180",
	"IN1767004430101",
	"IN2030004430071",
	"IN8420004430043",
	"IN9988004429974",
	"IN3136004429947",
	"IN1122004429933",
	"IN8449004429909",
	"IN4745004429886",
	"IN1888004429864",
	"IN4276004429826",
	"IN5578004429777",
	"IN1721004429768",
	"IN6298004429726",
	"IN1192004429703",
	"IN0064004429682",
	"IN8623004429675",
	"IN2915004429648",
	"IN6866004429625",
	"IN9198004429582",
	"IN6759004429571",
	"IN4916004429513",
	"IN1422004429464",
	"IN3796004429397",
	"IN1378004429360",
	"IN3321004429318",
	"IN6633004429298",
	"IN2832004429227",
	"IN0783004429189",
	"IN6423004429158",
	"IN8503004429100",
	"IN2075004429072",
	"IN1407004429032",
	"IN4286004428986",
	"IN6254004428938",
	"IN5652004428907",
	"IN2628004428888",
	"IN5533004428867",
	"IN1233004428832",
	"IN9900004428813",
	"IN3463004428773",
	"IN7991004428710",
	"IN1991004428676",
	"IN6669004428661",
	"IN9145004428645",
	"IN8473004428628",
	"IN8423004428595",
	"IN1033004428556",
	"IN3965004428533",
	"IN0486004428472",
	"IN0987004428434",
	"IN3493004428424",
	"IN0128004428390",
	"IN9047004428348",
	"IN3059004428316",
	"IN9195004428255",
	"IN8774004428212",
	"IN2111004428185",
	"IN6785004428152",
	"IN3645004428136",
	"IN2695004428114",
	"IN3287004428091",
	"IN8687004428069",
	"IN6488004428032",
	"IN5893004428021",
	"IN0357004428000",
	"IN7410004427981",
	"IN2242004427955",
	"IN4787004427913",
	"IN0105004427864",
	"IN7140004427841",
	"IN3799004427797",
	"IN4487004427771",
	"IN9893004427755",
	"IN3942004427732",
	"IN1578004427701",
	"IN6992004427657",
	"IN5858004427593",
	"IN6180004427575",
	"IN8140004427554",
	"IN5887004427531",
	"IN7056004427514",
	"IN9317004427496",
	"IN8550004427470",
	"IN0791004427412",
	"IN8497004427395",
	"IN0854004427361",
	"IN4672004427294",
	"IN4786004427271",
	"IN0825004427233",
	"IN1455004427202",
	"IN5449004427165",
	"IN1357004427134",
	"IN5897004427094",
	"IN0428004427075",
	"IN2976004427051",
	"IN5964004427024",
	"IN4580004427006",
	"IN6024004426963",
	"IN1217004426917",
	"IN4908004426901",
	"IN7168004426818",
	"IN9948004426790",
	"IN4916004426774",
	"IN9780004426722",
	"IN8970004426704",
	"IN6447004426671",
	"IN3313004426632",
	"IN4224004426605",
	"IN9178004426586",
	"IN9746004426569",
	"IN2219004426548",
	"IN2461004426524",
	"IN2509004426513",
	"IN3797004426425",
	"IN2344004426380",
	"IN2774004426367",
	"IN7108004426339",
	"IN2616004426280",
	"IN8183004426250",
	"IN2868004426230",
	"IN7745004426214",
	"IN9527004426172",
	"IN4125004426143",
	"IN2236004426130",
	"IN0533004426099",
	"IN5226004426075",
	"IN8297004426032",
	"IN4962004426000",
	"IN9248004425954",
	"IN9844004425936",
	"IN8860004425890",
	"IN6119004425860",
	"IN5467004425793",
	"IN9073004425737",
	"IN5479004425718",
	"IN2317004425682",
	"IN0940004425608",
	"IN7688004425570",
	"IN4549004425550",
	"IN9290004425504",
	"IN0340004425485",
	"IN0871004425475",
	"IN7962004425447",
	"IN4728004425418",
	"IN7753004425390",
	"IN1704004425363",
	"IN7648004425330",
	"IN1751004425287",
	"IN1167004425216",
	"IN8909004425184",
	"IN2261004425165",
	"IN4914004425126",
	"IN8126004425055",
	"IN4355004425020",
	"IN5064004425009",
	"IN6401004424983",
	"IN8922004424949",
	"IN6704004424893",
	"IN8412004424861",
	"IN9419004424835",
	"IN8726004424803",
	"IN4928004424776",
	"IN3016004424761",
	"IN1946004424725",
	"IN5608004424611",
	"IN3986004424592",
	"IN3967004424576",
	"IN9071004424522",
	"IN5784004424477",
	"IN2801004424365",
	"IN9385004424344",
	"IN1768004424315",
	"IN8074004424296",
	"IN1578004424211",
	"IN7661004424197",
	"IN7461004424185",
	"IN2291004424164",
	"IN4100004424138",
	"IN0228004424127",
	"IN0125004424091",
	"IN9665004424068",
	"IN5644004424018",
	"IN8417004423994",
	"IN1435004423981",
	"IN0567004423964",
	"IN6547004423942",
	"IN4477004423904",
	"IN3303004423855",
	"IN3939004423798",
	"IN0242004423754",
	"IN9477004423732",
	"IN8654004423672",
	"IN1376004423636",
	"IN9039004423620",
	"IN8580004423606",
	"IN8230004423583",
	"IN2518004423488",
	"IN5264004423462",
	"IN9350004423442",
	"IN8002004423421",
	"IN2557004423381",
	"IN7476004423362",
	"IN4838004423344",
	"IN9871004423331",
	"IN8337004423292",
	"IN7082004423272",
	"IN3003004423224",
	"IN0550004423182",
	"IN1273004423167",
	"IN2260004423143",
	"IN6089004423113",
	"IN4483004423091",
	"IN0504004423075",
	"IN9552004423048",
	"IN3173004423016",
	"IN5802004422995",
	"IN9604004422933",
	"IN7920004422911",
	"IN4257004422892",
	"IN7278004422863",
	"IN3954004422850",
	"IN7030004422821",
	"IN1097004422771",
	"IN1117004422745",
	"IN9785004422710",
	"IN2914004422693",
	"IN3451004422624",
	"IN2157004422599",
	"IN8954004422571",
	"IN5643004422512",
	"IN6699004422475",
	"IN0643004422355",
	"IN0555004422315",
	"IN6806004422283",
	"IN2094004422252",
	"IN6747004422244",
	"IN5769004422221",
	"IN4683004422166",
	"IN4768004422128",
	"IN9692004422046",
	"IN0712004421956",
	"IN1419004421946",
	"IN1606004421923",
	"IN2592004421898",
	"IN8155004421866",
	"IN3310004421852",
	"IN5962004421772",
	"IN6304004421743",
	"IN5790004421708",
	"IN6895004421648",
	"IN6577004421625",
	"IN4458004421586",
	"IN6614004421552",
	"IN1029004421533",
	"IN8840004421484",
	"IN9968004421443",
	"IN1321004421359",
	"IN7132004421342",
	"IN6381004421327",
	"IN7119004421273",
	"IN3156004421229",
	"IN0252004421189",
	"IN6867004421156",
	"IN6180004421115",
	"IN9748004421090",
	"IN8224004421075",
	"IN7045004421044",
	"IN9179004421021",
	"IN6179004420992",
	"IN8152004420937",
	"IN2406004420920",
	"IN0351004420866",
	"IN2694004420825",
	"IN3055004420794",
	"IN9405004420778",
}
