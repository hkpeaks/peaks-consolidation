package main

import (
	"fmt"
	"path/filepath"
	"peakgo/peakgo"
	"time"
)

func main() {

	StartTime := time.Now()

	var df peakgo.Dataframe
	df.LogFileName = "Outbox/Peakgo-Log-" + StartTime.Format("060102-150405") + ".csv"
	df.PartitionSizeMB = 5
	df.Thread = 100

	peakgo.CreateLog(df)

	query1 := peakgo.Query(
        "filter", "Style(=F)",
		"build_key_value", "Product, Style => Table(key_value)")

	master_df := peakgo.RunBatch(df, "Inbox/Master.csv", query1)

	query2 := peakgo.Query(
		"filter", "Shop(S77..S78)",
		"join_key_value", "Product, Style => Inner(key_value)",
		"add_column", "Quantity, Unit_Price => Multiply(Amount)",
		"filter", "Amount:Float(>100000)",
		"group_by", "Shop, Product => Count() Sum(Quantity) Sum(Amount)",
	)

	source_file := peakgo.GetCliFilePath("10-MillionRows.csv")
	result_file := []string{"Outbox/PeakGo-Detail-Result-" + filepath.Base(source_file),
		"Outbox/Peakgo-Summary-Result-" + filepath.Base(source_file)}

	peakgo.RunStream(df, &master_df, source_file, query2, result_file)

	peakgo.ViewSample(result_file[0])
	peakgo.ViewSample(result_file[1])

	EndTime := time.Now()
	duration := EndTime.Sub(StartTime)
	fmt.Printf("Peakgo Duration (In Second): %.3f \n", duration.Seconds())
}
