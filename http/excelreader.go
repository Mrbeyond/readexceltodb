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

	if err != nil {
		processPool.errorMessage = err.Error()
		close(processPool.dataChan) // close data channel
		return
	}

	totalRows := len(rows[1:])
	totalChunks := (totalRows + chunkVolume - 1) / chunkVolume
	fmt.Println(totalChunks)

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
	fmt.Println("Here and here ")
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
					fmt.Println(err.Error(), "is error")
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

	fmt.Println("large")

	close(processPool.dataChan)
}

// processExcelChunk concurrently processes chunks of rows from the data channel
func (processPool *ProcessPool) processExcelChunk() {
	for i := 0; i < processPool.numWorkers; i++ {
		go func() {
			defer processPool.wg.Done()
			for rows := range processPool.dataChan {
				// Process each chunk here, such as inserting into database
				// processPool.mutex.Lock()
				fmt.Println(rows)
				// processPool.mutex.Unlock()

				// Check if all the Excel lines have been saved
				processPool.mutex.Lock()
				processPool.totalChunkFetched += len(rows)
				fmt.Println(processPool.totalChunkFetched)
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

	// Read and get header
	iteRows.Next()
	header, err := iteRows.Columns()
	// Use header for more dynmic purpose e.g particular column process
	fmt.Sprintln(header)
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
	if fileVolume > 10 {
		processPool.generateExcelChunkLV(iteRows) // Read excel rows into chunks
	} else {
		processPool.generateExcelChunkSV(xclF, sheetName) // Read excel rows into chunksv
	}

	fmt.Println("Here")
	// Lock flow until all workers are done
	wg.Wait()

	ctx.JSON(http.StatusOK, gin.H{
		"readline":      processPool.excelLinesRead,
		"processedline": processPool.totalChunkFetched,
		"iscompleted":   processPool.done,
	})

}
