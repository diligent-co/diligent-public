//go:build !ios && !android

package metrics

import (
	"coinbot/src/datamodels"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	fyne "fyne.io/fyne/v2"
	fyneApp "fyne.io/fyne/v2/app"
	fyneCanvas "fyne.io/fyne/v2/canvas"
	fyneTheme "fyne.io/fyne/v2/theme"
	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/plotutil"
	"gonum.org/v1/plot/vg"
	"gonum.org/v1/plot/vg/draw"
	"gonum.org/v1/plot/vg/vgimg"
)

type MetricPlotter struct {
	liveWindow     int
	updateDataChan chan struct{}
	metrics        []datamodels.Metric
	plot           *plot.Plot
	appOutput      bool
	fileOutput     bool
	filename       string
	mutex          sync.RWMutex
	window         fyne.Window
	image          *fyneCanvas.Image
	stopChan       chan struct{}
}

func NewMetricPlotter() *MetricPlotter {
	return &MetricPlotter{
		liveWindow:     1000,
		updateDataChan: make(chan struct{}, 1),
		stopChan:       make(chan struct{}, 1),
	}
}

func (pb *MetricPlotter) WithLiveWindow(liveWindow int) *MetricPlotter {
	pb.liveWindow = liveWindow
	return pb
}

func (pb *MetricPlotter) WithMetrics(metrics []datamodels.Metric) *MetricPlotter {
	pb.metrics = metrics
	return pb
}

func (pb *MetricPlotter) WithFileOutput(filename string) *MetricPlotter {
	pb.filename = filename
	pb.fileOutput = true
	return pb
}

func (pb *MetricPlotter) WithAppOutput() *MetricPlotter {
	pb.appOutput = true
	return pb
}

func (pb *MetricPlotter) Build() *MetricPlotter {
	// verify that metrics are set
	if len(pb.metrics) == 0 {
		fmt.Println("No metrics to plot")
		return pb
	}

	// verify that either file or app output is set
	if !pb.fileOutput && !pb.appOutput {
		fmt.Println("No output type set")
		return pb
	}

	// verify that filename is set if file output is set
	if pb.fileOutput && pb.filename == "" {
		fmt.Println("Filename is not set")
		return pb
	}

	return pb
}

func (pb *MetricPlotter) UpdateMetrics(metrics []datamodels.Metric) {
	pb.mutex.Lock()
	defer pb.mutex.Unlock()
	pb.metrics = metrics

	// trigger update
	select {
	case pb.updateDataChan <- struct{}{}:
	default:
	}
}

func (pb *MetricPlotter) Stop() {
	pb.stopChan <- struct{}{}
	close(pb.stopChan)
	close(pb.updateDataChan)
}

func (pb *MetricPlotter) Plot() {
    if pb.appOutput {
        pb.plotAppLive()
    } else {
        pb.plotStatic()
	}
}

func (pb *MetricPlotter) plotStatic() {
	fmt.Println("Plotting metrics")

	// time stamps are x axis
	// all other metrics are y axis

	firstMetric := pb.metrics[0]

	var p *plot.Plot

	switch firstMetric.MetricGeneratorType {
	case datamodels.MetricGeneratorTypePortfolio:
		portfolioMetrics := make([]datamodels.PortfolioRealTimeMetrics, len(pb.metrics))
		for i, metric := range pb.metrics {
			err := json.Unmarshal(metric.MetricValue, &portfolioMetrics[i])
			if err != nil {
				fmt.Println("Error unmarshalling portfolio metrics:", err)
				return
			}
		}
		p = plotPortfolioMetrics(portfolioMetrics)

	case datamodels.MetricGeneratorTypeStrategy:
		strategyMetrics := make([]datamodels.Signal, len(pb.metrics))
		for i, metric := range pb.metrics {
			err := json.Unmarshal(metric.MetricValue, &strategyMetrics[i])
			if err != nil {
				fmt.Println("Error unmarshalling strategy metrics:", err)
				return
			}
		}
		p = plotStrategyMetrics(strategyMetrics)
	default:
		fmt.Println("Unknown metric generator type")
		return
	}

	pb.plot = p

	if pb.fileOutput {
		slog.Info("MetricPlotter plotting via file", "filename", pb.filename)
		// create directory if it doesn't exist
		os.MkdirAll(filepath.Dir(pb.filename), 0755)
		saveErr := p.Save(10*vg.Inch, 10*vg.Inch, pb.filename)
		if saveErr != nil {
			slog.Error("Error saving plot to file", "error", saveErr)
		}
	}

	if pb.appOutput {
		slog.Info("MetricPlotter plotting via app")
		plotApp(p)
	}
}

func (pb *MetricPlotter) plotAppLive() {
	if !pb.hasMetrics() {
		slog.Info("Plotter waiting for metrics to plot")
		<-pb.updateDataChan
	}
	slog.Info("Plotter got metrics to plot")

	a := fyneApp.New()
	a.Settings().SetTheme(fyneTheme.Current())

	pb.window = a.NewWindow("Live Plot")
	pb.image = fyneCanvas.NewImageFromImage(nil)
	pb.image.FillMode = fyneCanvas.ImageFillOriginal
	pb.image.SetMinSize(fyne.NewSize(800, 600))

	pb.window.SetContent(pb.image)
	pb.window.Resize(fyne.NewSize(800, 600))

	ticker := time.NewTicker(time.Second / 30) // 30 FPS
	defer ticker.Stop()

	// Start update goroutine
	go func() {
		for {
			select {
			case <-pb.stopChan:
				return
			case <-pb.updateDataChan:
				slog.Info("Plotter got metric update notification")
				pb.renderPlot()
				pb.window.Canvas().Refresh(pb.image)
            }
        }
    }()

	pb.renderPlot()
    
    pb.window.Show()
    a.Run()
}

func (pb *MetricPlotter) renderPlot() {
	metrics := pb.getMetrics()

	if len(metrics) == 0 {
		slog.Info("No metrics to plot")
		return
	}
    
    var p *plot.Plot
    switch metrics[0].MetricGeneratorType {
    case datamodels.MetricGeneratorTypePortfolio:
        portfolioMetrics := make([]datamodels.PortfolioRealTimeMetrics, len(metrics))
        for i, metric := range metrics {
            json.Unmarshal(metric.MetricValue, &portfolioMetrics[i])
        }
        p = plotPortfolioMetrics(portfolioMetrics)
    case datamodels.MetricGeneratorTypeStrategy:
        strategyMetrics := make([]datamodels.Signal, len(metrics))
        for i, metric := range metrics {
            json.Unmarshal(metric.MetricValue, &strategyMetrics[i])
        }
        p = plotStrategyMetrics(strategyMetrics)
    }
    
    if p != nil {
        c := vgimg.New(vg.Points(800), vg.Points(600))
        p.Draw(draw.New(c))
        pb.image.Image = c.Image()
        pb.image.Refresh()
    }
}

func plotPortfolioMetrics(metrics []datamodels.PortfolioRealTimeMetrics) *plot.Plot {
	fmt.Println("Plotting portfolio metrics")

	// Create main plot without axes
	p := plot.New()
	p.Title.Text = "Portfolio Metrics"
	p.X.LineStyle.Width = 0
	p.Y.LineStyle.Width = 0
	p.X.Label.Text = ""
	p.Y.Label.Text = ""
	p.X.Tick.Length = 0
	p.Y.Tick.Length = 0

	// Prepare the data
	x := make([]time.Time, len(metrics))
	metricsData := map[string][]float64{
		"Returns":      make([]float64, len(metrics)),
		"PortfolioGrowthPct": make([]float64, len(metrics)),
		"CashBalance":  make([]float64, len(metrics)),
		"TotalValue":   make([]float64, len(metrics)),
		"MaxDrawdown":  make([]float64, len(metrics)),
	}

	// make dicts for assets
	m0 := metrics[0]
	assets := make([]string, len(m0.Allocations))
	i := 0
	for asset := range m0.Allocations {
		assets[i] = string(asset)
		i++
	}
	sortedAssets := sort.StringSlice(assets)
	sortedAssets.Sort()
	for _, asset := range sortedAssets {
		metricsData[asset+"Allocation"] = make([]float64, len(metrics))
	}

	metricKeys := []string{"Returns", "PortfolioGrowthPct", "CashBalance", "TotalValue", "MaxDrawdown"}
	for _, asset := range sortedAssets {
		metricKeys = append(metricKeys, asset+"Allocation")
	}
	metricYRanges := map[string][]float64{
		"Returns":      {-0.1, 0.1},
		"PortfolioGrowthPct": {-0.1, 0.1},
		"CashBalance":  {0, 15000},
		"TotalValue":   {0, 15000},
		"MaxDrawdown":  {0, 0.1},
	}
	for _, asset := range sortedAssets {
		metricYRanges[asset+"Allocation"] = []float64{0, 0.1}
	}


	for i, metric := range metrics {
		x[i] = metric.Timestamp
		metricsData["Returns"][i] = metric.PortfolioGrowthPct
		metricsData["CashBalance"][i] = metric.CashBalance
		metricsData["TotalValue"][i] = metric.TotalValue
		metricsData["MaxDrawdown"][i] = metric.Drawdown
		for _, asset := range sortedAssets {
			assetAllocation := metric.Allocations[datamodels.Asset(asset)]
			metricsData[asset+"Allocation"][i] = assetAllocation
		}
	}

	// Calculate grid dimensions, 3 columns, and as many rows as needed
	numMetrics := len(metricsData)
	rows := (numMetrics + 2) / 3 // Rounds up division to ensure enough rows
	cols := 3
	// Create grid layout
	t := draw.Tiles{
		Rows:      rows,
		Cols:      cols,
		PadX:      vg.Millimeter,
		PadY:      vg.Millimeter,
		PadTop:    vg.Points(10),
		PadBottom: vg.Points(10),
		PadLeft:   vg.Points(10),
		PadRight:  vg.Points(10),
	}

	// Create subplots
	plots := make([]*plot.Plot, numMetrics)
	for i := range plots {
		plots[i] = plot.New()
	}

	// Add data to each subplot
	i = 0
	for _, metricKey := range metricKeys {
		values := metricsData[metricKey]
		pts := make(plotter.XYs, len(x))
		for j := range pts {
			pts[j].X = float64(x[j].Unix())
			pts[j].Y = values[j]
		}

		line, err := plotter.NewLine(pts)
		if err != nil {
			fmt.Printf("Error creating line for %s: %v\n", metricKey, err)
			continue
		}
		line.Color = plotutil.Color(i % len(plotutil.DefaultColors))

		plots[i].Y.Min = metricYRanges[metricKey][0]
		plots[i].Y.Max = metricYRanges[metricKey][1]
		plots[i].Title.Text = metricKey
		plots[i].X.Label.Text = "Time"
		plots[i].Y.Label.Text = metricKey
		plots[i].Add(line)
		plots[i].X.Tick.Marker = plot.TimeTicks{Format: "2006-01-02\n15:04"}

		// Add grid lines
		plots[i].Add(plotter.NewGrid())

		i++
	}

	// Create the image and draw context
	img := vgimg.New(vg.Points(800), vg.Points(800))
	dc := draw.New(img)

	// Draw all subplots
	plotGrid := make([][]*plot.Plot, rows)
	for i := range plotGrid {
		plotGrid[i] = make([]*plot.Plot, cols)
	}

	// Fill the grid
	for i := 0; i < numMetrics; i++ {
		row := i / cols
		col := i % cols
		plotGrid[row][col] = plots[i]
	}

	// Draw all subplots with proper grid alignment
	canvases := plot.Align(plotGrid, t, dc)
	for i := 0; i < rows; i++ {
		for j := 0; j < cols; j++ {
			if plotGrid[i][j] != nil {
				plotGrid[i][j].Draw(canvases[i][j])
			}
		}
	}

	// Create a new plot that will contain the image
	finalPlot := plot.New()
	finalPlot.Title.Text = "Portfolio Metrics"

	// Add the image to the plot
	finalPlot.Add(plotter.NewImage(img.Image(), 0, 0, float64(vg.Points(800).Points()), float64(vg.Points(800).Points())))

	return finalPlot
}

func plotStrategyMetrics(metrics []datamodels.Signal) *plot.Plot {
	fmt.Println("Plotting strategy metrics")
	p := plot.New()

	return p
}

func plotApp(p *plot.Plot) {
	a := fyneApp.New()
	a.Settings().SetTheme(fyneTheme.Current())
	defer a.Quit()
	window := a.NewWindow("Plot")

	// Create a canvas with dimensions
	c := vgimg.New(vg.Points(800), vg.Points(600))

	// Draw the plot onto the canvas
	p.Draw(draw.New(c))

	// Convert the canvas to an image
	img := fyneCanvas.NewImageFromImage(c.Image())
	img.FillMode = fyneCanvas.ImageFillOriginal
	img.SetMinSize(fyne.NewSize(800, 600))

	window.SetContent(img)
	window.Resize(fyne.NewSize(800, 600))
	window.Show()

	a.Run()
}

func (pb *MetricPlotter) hasMetrics() bool {
	// mutex lock
	pb.mutex.RLock()
	defer pb.mutex.RUnlock()
	return len(pb.metrics) > 0
}

func (pb *MetricPlotter) getMetrics() []datamodels.Metric {
	// mutex lock
	pb.mutex.RLock()
	defer pb.mutex.RUnlock()
	metricsCopy := make([]datamodels.Metric, len(pb.metrics))
	for i, metric := range pb.metrics {
		metricsCopy[i] = metric
	}
	return metricsCopy
}
