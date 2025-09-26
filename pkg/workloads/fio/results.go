package fio

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"
)

// FIOResult represents the complete FIO JSON output
type FIOResult struct {
	FIOVersion    string                 `json:"fio version"`
	Timestamp     int64                  `json:"timestamp"`
	Time          string                 `json:"time"`
	GlobalOptions map[string]interface{} `json:"global options"`
	ClientStats   []ClientStats          `json:"client_stats"`
	DiskUtil      []interface{}          `json:"disk_util"`
}

// ClientStats represents individual client/job statistics
type ClientStats struct {
	JobName    string            `json:"jobname"`
	GroupID    int               `json:"groupid"`
	JobStart   int64             `json:"job_start"`
	Error      int               `json:"error"`
	JobOptions map[string]string `json:"job options"`
	Read       IOStats           `json:"read"`
	Write      IOStats           `json:"write"`
	Trim       IOStats           `json:"trim"`
	Sync       SyncStats         `json:"sync"`
	JobRuntime int               `json:"job_runtime"`
	UserCPU    float64           `json:"usr_cpu"`
	SysCPU     float64           `json:"sys_cpu"`
	Ctx        int               `json:"ctx"`
	MajF       int               `json:"majf"`
	MinF       int               `json:"minf"`
	Hostname   string            `json:"hostname"`
	Port       int               `json:"port"`
}

// IOStats represents read/write/trim statistics
type IOStats struct {
	IOBytes     int64    `json:"io_bytes"`
	IOKBytes    int64    `json:"io_kbytes"`
	BWBytes     int64    `json:"bw_bytes"`
	BW          int      `json:"bw"`
	IOPS        float64  `json:"iops"`
	Runtime     int      `json:"runtime"`
	TotalIOs    int      `json:"total_ios"`
	ShortIOs    int      `json:"short_ios"`
	DropIOs     int      `json:"drop_ios"`
	SlatNs      LatStats `json:"slat_ns"`
	ClatNs      LatStats `json:"clat_ns"`
	LatNs       LatStats `json:"lat_ns"`
	BWMin       int      `json:"bw_min"`
	BWMax       int      `json:"bw_max"`
	BWAgg       float64  `json:"bw_agg"`
	BWMean      float64  `json:"bw_mean"`
	BWDev       float64  `json:"bw_dev"`
	BWSamples   int      `json:"bw_samples"`
	IOPSMin     int      `json:"iops_min"`
	IOPSMax     int      `json:"iops_max"`
	IOPSMean    float64  `json:"iops_mean"`
	IOPSStddev  float64  `json:"iops_stddev"`
	IOPSSamples int      `json:"iops_samples"`
}

// LatStats represents latency statistics
type LatStats struct {
	Min        int64                  `json:"min"`
	Max        int64                  `json:"max"`
	Mean       float64                `json:"mean"`
	Stddev     float64                `json:"stddev"`
	N          int                    `json:"N"`
	Percentile map[string]interface{} `json:"percentile,omitempty"`
}

// SyncStats represents sync operation statistics
type SyncStats struct {
	TotalIOs int      `json:"total_ios"`
	LatNs    LatStats `json:"lat_ns"`
}

// ResultSummary represents a simplified view of results for table display
type ResultSummary struct {
	TestID      string
	Sample      int // Sample number/iteration
	JobName     string
	Hostname    string
	ReadIOPS    float64
	ReadBW      int // KB/s
	WriteIOPS   float64
	WriteBW     int     // KB/s
	ReadLatP50  float64 // microseconds
	ReadLatP95  float64 // microseconds
	WriteLatP50 float64 // microseconds
	WriteLatP95 float64 // microseconds
	Runtime     int     // seconds
}

// ParseFIOResults parses FIO JSON results from log output
func ParseFIOResults(logOutput string) ([]*FIOResult, error) {
	var results []*FIOResult

	// Use a more robust approach to extract JSON blocks
	lines := strings.Split(logOutput, "\n")
	var currentJSON strings.Builder
	var testID string
	inJSONBlock := false

	for _, line := range lines {
		line = strings.TrimSpace(line)

		// Check for start of FIO result block
		if strings.HasPrefix(line, "FIO Result for ") {
			testID = strings.TrimPrefix(line, "FIO Result for ")
			inJSONBlock = true
			currentJSON.Reset()
			continue
		}

		// Check for end of FIO result block
		if strings.HasPrefix(line, "END FIO Result for ") {
			if inJSONBlock && currentJSON.Len() > 0 {
				// Parse the accumulated JSON
				var result FIOResult
				if err := json.Unmarshal([]byte(currentJSON.String()), &result); err != nil {
					fmt.Printf("Warning: Failed to parse FIO JSON for test %s: %v\n", testID, err)
				} else {
					results = append(results, &result)
				}
			}
			inJSONBlock = false
			currentJSON.Reset()
			continue
		}

		// Accumulate JSON content
		if inJSONBlock {
			currentJSON.WriteString(line)
			currentJSON.WriteString("\n")
		}
	}

	return results, nil
}

// ExtractResultSummaries converts FIO results to simplified summaries
func ExtractResultSummaries(results []*FIOResult, testID string) []ResultSummary {
	var summaries []ResultSummary

	for sampleIdx, result := range results {
		for _, client := range result.ClientStats {
			// Skip "All clients" summary entries
			if client.JobName == "All clients" {
				continue
			}

			summary := ResultSummary{
				TestID:   testID,
				Sample:   sampleIdx + 1, // Start sample numbering from 1
				JobName:  client.JobName,
				Hostname: client.Hostname,
				Runtime:  client.JobRuntime / 1000, // Convert ms to seconds
			}

			// Extract read stats
			if client.Read.TotalIOs > 0 {
				summary.ReadIOPS = client.Read.IOPS
				summary.ReadBW = client.Read.BW

				// Extract percentiles (convert from nanoseconds to microseconds)
				if p50, ok := client.Read.ClatNs.Percentile["50.000000"].(float64); ok {
					summary.ReadLatP50 = p50 / 1000.0
				}
				if p95, ok := client.Read.ClatNs.Percentile["95.000000"].(float64); ok {
					summary.ReadLatP95 = p95 / 1000.0
				}
			}

			// Extract write stats
			if client.Write.TotalIOs > 0 {
				summary.WriteIOPS = client.Write.IOPS
				summary.WriteBW = client.Write.BW

				// Extract percentiles (convert from nanoseconds to microseconds)
				if p50, ok := client.Write.ClatNs.Percentile["50.000000"].(float64); ok {
					summary.WriteLatP50 = p50 / 1000.0
				}
				if p95, ok := client.Write.ClatNs.Percentile["95.000000"].(float64); ok {
					summary.WriteLatP95 = p95 / 1000.0
				}
			}

			summaries = append(summaries, summary)
		}
	}

	return summaries
}

// PrintResultsTable prints FIO results in a formatted table
func PrintResultsTable(summaries []ResultSummary) {
	if len(summaries) == 0 {
		fmt.Println("No FIO results found")
		return
	}

	// Create a tab writer for aligned columns
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)

	// Print header
	fmt.Fprintf(w, "\n=== FIO Benchmark Results ===\n")
	fmt.Fprintf(w, "Test ID\tSample\tJob\tHostname\tRead IOPS\tRead BW (KB/s)\tWrite IOPS\tWrite BW (KB/s)\tRead Lat P50 (μs)\tRead Lat P95 (μs)\tWrite Lat P50 (μs)\tWrite Lat P95 (μs)\tRuntime (s)\n")
	fmt.Fprintf(w, "-------\t------\t---\t--------\t---------\t-----------\t----------\t------------\t--------------\t--------------\t---------------\t---------------\t-----------\n")

	// Print data rows
	for _, summary := range summaries {
		fmt.Fprintf(w, "%s\t%d\t%s\t%s\t%.1f\t%d\t%.1f\t%d\t%.1f\t%.1f\t%.1f\t%.1f\t%d\n",
			summary.TestID,
			summary.Sample,
			summary.JobName,
			summary.Hostname,
			summary.ReadIOPS,
			summary.ReadBW,
			summary.WriteIOPS,
			summary.WriteBW,
			summary.ReadLatP50,
			summary.ReadLatP95,
			summary.WriteLatP50,
			summary.WriteLatP95,
			summary.Runtime,
		)
	}

	w.Flush()
	fmt.Println()
}

// ExportResultsToCSV exports FIO results to a CSV file
func ExportResultsToCSV(summaries []ResultSummary, filename string) error {
	if len(summaries) == 0 {
		return fmt.Errorf("no results to export")
	}

	// Create the CSV file
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create CSV file %s: %w", filename, err)
	}
	defer file.Close()

	// Create CSV writer
	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header
	header := []string{
		"Test ID",
		"Sample",
		"Job Type",
		"Hostname",
		"Read IOPS",
		"Read BW (KB/s)",
		"Write IOPS",
		"Write BW (KB/s)",
		"Read Lat P50 (μs)",
		"Read Lat P95 (μs)",
		"Write Lat P50 (μs)",
		"Write Lat P95 (μs)",
		"Runtime (s)",
		"Timestamp",
	}

	if err := writer.Write(header); err != nil {
		return fmt.Errorf("failed to write CSV header: %w", err)
	}

	// Write data rows
	timestamp := time.Now().Format("2006-01-02 15:04:05")
	for _, summary := range summaries {
		record := []string{
			summary.TestID,
			strconv.Itoa(summary.Sample),
			summary.JobName,
			summary.Hostname,
			strconv.FormatFloat(summary.ReadIOPS, 'f', 1, 64),
			strconv.Itoa(summary.ReadBW),
			strconv.FormatFloat(summary.WriteIOPS, 'f', 1, 64),
			strconv.Itoa(summary.WriteBW),
			strconv.FormatFloat(summary.ReadLatP50, 'f', 1, 64),
			strconv.FormatFloat(summary.ReadLatP95, 'f', 1, 64),
			strconv.FormatFloat(summary.WriteLatP50, 'f', 1, 64),
			strconv.FormatFloat(summary.WriteLatP95, 'f', 1, 64),
			strconv.Itoa(summary.Runtime),
			timestamp,
		}

		if err := writer.Write(record); err != nil {
			return fmt.Errorf("failed to write CSV record: %w", err)
		}
	}

	return nil
}

// CaptureFIOResults captures and parses FIO results from log output
func CaptureFIOResults(logOutput, testID string) {
	CaptureFIOResultsWithOptions(logOutput, testID, true) // Default: export to CSV
}

// CaptureFIOResultsWithOptions captures and parses FIO results with export options
func CaptureFIOResultsWithOptions(logOutput, testID string, exportCSV bool) {
	results, err := ParseFIOResults(logOutput)
	if err != nil {
		fmt.Printf("Error parsing FIO results: %v\n", err)
		return
	}

	if len(results) == 0 {
		fmt.Println("No FIO results found in output")
		return
	}

	fmt.Printf("Found %d FIO result(s)\n", len(results))

	// Extract and display summaries
	summaries := ExtractResultSummaries(results, testID)
	PrintResultsTable(summaries)

	// Export to CSV if requested
	if exportCSV {
		csvFilename := fmt.Sprintf("fio-results-%s-%s.csv", testID, time.Now().Format("20060102-150405"))
		if err := ExportResultsToCSV(summaries, csvFilename); err != nil {
			fmt.Printf("Warning: Failed to export results to CSV: %v\n", err)
		} else {
			fmt.Printf("Results exported to: %s\n", csvFilename)
		}
	}
}

// ParseFIOResultsFromReader parses FIO results from a reader (e.g., pod logs)
func ParseFIOResultsFromReader(reader *bufio.Scanner, testID string) {
	var logOutput strings.Builder

	for reader.Scan() {
		line := reader.Text()
		logOutput.WriteString(line)
		logOutput.WriteString("\n")

		// Print the line as it comes (real-time output)
		fmt.Println(line)

		// Check if we have a complete FIO result block
		if strings.Contains(line, "END FIO Result for") {
			// Process any accumulated results
			CaptureFIOResults(logOutput.String(), testID)
		}
	}

	// Process any remaining results
	if logOutput.Len() > 0 {
		CaptureFIOResults(logOutput.String(), testID)
	}
}
