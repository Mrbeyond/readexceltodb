package http

import (
	"log"
	"math"
	"os"
	"runtime"
	"sync"
	"testing"

	"github.com/joho/godotenv"
	"github.com/xuri/excelize/v2"
)

var filePath string

func init() {

	err := godotenv.Load("../.env")
	if err != nil {
		log.Fatal(err)
	}

	filePath = os.Getenv("EXCEL_FILE_PATH")
}

// BenchmarkGenerateExcelChunkLV benchmarks the GenerateExcelChunkLV function
func BenchmarkGenerateExcelChunkLV(b *testing.B) {
	// Run the benchmark
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Open XLSX file using excelize
		file, err := os.Open(filePath)
		if err != nil {
			b.Fatal(err)
		}
		defer file.Close()

		xclF, err := excelize.OpenReader(file)
		if err != nil {
			b.Fatal(err)
		}
		defer xclF.Close()

		// Get the first sheet name
		sheetName := xclF.GetSheetName(0)

		// Read header row to begin real data rows reading in GenerateExcelChunkLV
		iteRows, err := xclF.Rows(sheetName)
		if err != nil {
			b.Fatal(err)
		}
		iteRows.Next()
		_, err = iteRows.Columns()
		if err != nil {
			b.Fatal(err)
		}

		// Measure performance
		b.StartTimer()

		fileInfo, err := file.Stat()
		if err != nil {
			b.Fatal(err)
		}
		fileVolume := fileInfo.Size() / MB

		numWorkers := int(math.Min(float64((fileVolume+1)*10), float64(runtime.NumCPU()*10)))
		var wg sync.WaitGroup
		wg.Add(numWorkers)

		// Create a new ProcessPool instance
		processPool := ProcessPool{
			wg:         &wg,
			dataChan:   make(chan []string, numWorkers),
			numWorkers: numWorkers,
		}

		go processPool.processExcelChunk()

		processPool.generateExcelChunkLV(iteRows)

		wg.Wait()
		b.StopTimer()
	}
}

// BenchmarkGenerateExcelChunkSV benchmarks the GenerateExcelChunkSV function
func BenchmarkGenerateExcelChunkSV(b *testing.B) {
	// Run the benchmark
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Open XLSX file using excelize
		file, err := os.Open(filePath)
		if err != nil {
			b.Fatal(err)
		}
		defer file.Close()

		xclF, err := excelize.OpenReader(file)
		if err != nil {
			b.Fatal(err)
		}
		defer xclF.Close()

		// Get the first sheet name
		sheetName := xclF.GetSheetName(0)

		// Measure performance
		b.StartTimer()

		fileInfo, err := file.Stat()
		if err != nil {
			b.Fatal(err)
		}
		fileVolume := fileInfo.Size() / MB

		numWorkers := int(math.Min(float64((fileVolume+1)*10), float64(runtime.NumCPU()*10)))
		var wg sync.WaitGroup
		wg.Add(numWorkers)

		// Create a new ProcessPool instance
		processPool := ProcessPool{
			wg:         &wg,
			dataChan:   make(chan []string, numWorkers),
			numWorkers: numWorkers,
		}

		go processPool.processExcelChunk()

		processPool.generateExcelChunkSV(xclF, sheetName)

		wg.Wait()
		b.StopTimer()
	}
}
