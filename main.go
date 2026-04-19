// Package main implements a commodity price web scraper using the Colly library.
// Scraped data (timestamp, commodity name, price) is persisted to a local SQLite
// database so the operator can query historical price trends without needing an
// external database server.
//
// The scraper is designed to be memory-efficient for a VPS with ≤1 GB RAM:
//   - Results are written to SQLite one row at a time (no large in-memory buffers).
//   - Colly's MaxBodySize is capped so oversized pages never fill RAM.
//   - Only one goroutine performs I/O at a time (Parallelism = 1).
//
// # Configuration (environment variables)
//
//	SCRAPER_TARGET_URLS  Comma-separated list of URLs to scrape.
//	                     Overrides the TargetURL constant.
//	                     Example: "https://site1.com/prices,https://site2.com/prices"
//
//	SCRAPER_INTERVAL     How often to repeat the scrape cycle automatically.
//	                     Accepts any Go duration string: "30m", "1h", "6h", etc.
//	                     Set to "0" to run once and exit (useful with external cron).
//	                     Default: 1h
//
// When the target website is known, update TargetURL, NameSelector, and
// PriceSelector accordingly.
package main

import (
	"database/sql"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gocolly/colly/v2"
	_ "github.com/mattn/go-sqlite3" // SQLite driver registered via side-effect import
)

// ---------------------------------------------------------------------------
// Configuration – update these values to point at the real commodity site.
// ---------------------------------------------------------------------------

const (
	// TargetURL is the fallback page used when SCRAPER_TARGET_URLS is not set.
	// Replace with the actual URL once the website is confirmed.
	TargetURL = "https://example.com/commodities"

	// NameSelector is the CSS selector that matches each commodity name element.
	// Example: "table.prices td.commodity-name"
	NameSelector = "td.commodity-name"

	// PriceSelector is the CSS selector that matches each commodity price element.
	// Example: "table.prices td.commodity-price"
	PriceSelector = "td.commodity-price"

	// DBFile is the path to the SQLite database file on disk.
	DBFile = "commodities.db"

	// MinDelaySec is the minimum number of seconds to wait between requests.
	MinDelaySec = 2

	// MaxDelaySec is the maximum number of seconds to wait between requests.
	MaxDelaySec = 5

	// DefaultInterval is the automatic re-scrape cadence when SCRAPER_INTERVAL
	// is not set.
	DefaultInterval = 1 * time.Hour
)

// rng is a package-level random source used for User-Agent rotation.
// It is initialised once in main() with a time-based seed so the sequence
// differs on every run.
var rng *rand.Rand

// userAgents is a pool of realistic desktop browser User-Agent strings.
// Colly will pick one at random for each request, making the scraper less
// likely to be blocked by basic bot-detection filters.
var userAgents = []string{
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
	"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
}

// CommodityPrice holds a single scraped data point before it is written to
// the database.
type CommodityPrice struct {
	Timestamp time.Time
	Name      string
	Price     string
}

// ---------------------------------------------------------------------------
// Environment helpers
// ---------------------------------------------------------------------------

// resolveURLs returns the list of target URLs to scrape.
// It reads SCRAPER_TARGET_URLS (comma-separated) from the environment.
// If that variable is absent it falls back to the TargetURL constant, and
// logs a warning when the constant is still the placeholder value.
func resolveURLs() []string {
	if raw := os.Getenv("SCRAPER_TARGET_URLS"); raw != "" {
		var urls []string
		// Split on comma; trim whitespace so "url1, url2" works too.
		for _, u := range strings.Split(raw, ",") {
			u = strings.TrimSpace(u)
			if u != "" {
				urls = append(urls, u)
			}
		}
		if len(urls) > 0 {
			log.Printf("scraping %d URL(s) from SCRAPER_TARGET_URLS", len(urls))
			return urls
		}
	}

	// Legacy single-URL variable (kept for backward compatibility).
	if u := strings.TrimSpace(os.Getenv("SCRAPER_TARGET_URL")); u != "" {
		log.Printf("scraping 1 URL from SCRAPER_TARGET_URL: %s", u)
		return []string{u}
	}

	if TargetURL == "https://example.com/commodities" {
		log.Println("WARNING: TargetURL is still the placeholder. " +
			"Set SCRAPER_TARGET_URLS or update the TargetURL constant before running in production.")
	}
	return []string{TargetURL}
}

// resolveInterval parses the SCRAPER_INTERVAL environment variable into a
// time.Duration.
//   - If unset, DefaultInterval (1 h) is used.
//   - If set to "0", the scraper runs once and exits (useful with external cron).
//   - Any valid Go duration string is accepted: "30m", "2h", "6h30m", etc.
func resolveInterval() time.Duration {
	raw := strings.TrimSpace(os.Getenv("SCRAPER_INTERVAL"))
	if raw == "" {
		return DefaultInterval
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		log.Printf("invalid SCRAPER_INTERVAL %q: %v – using default %v", raw, err, DefaultInterval)
		return DefaultInterval
	}
	return d
}

// ---------------------------------------------------------------------------
// Database helpers
// ---------------------------------------------------------------------------

// openDB opens (or creates) the SQLite database file and ensures the
// commodity_prices table exists.  The table schema stores:
//   - id          – auto-incremented primary key
//   - scraped_at  – ISO-8601 timestamp recorded by the scraper
//   - name        – commodity name as scraped from the page
//   - price       – price string as scraped (e.g. "1,234.56 USD")
func openDB(path string) (*sql.DB, error) {
	// sql.Open is lazy; the file is created on first use if it does not exist.
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}

	// Keep the connection pool small to reduce memory usage on low-RAM VPS.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	// Create the table if it does not already exist.
	// Using CREATE TABLE IF NOT EXISTS makes this idempotent – safe to call on
	// every startup without losing existing data.
	createTable := `
	CREATE TABLE IF NOT EXISTS commodity_prices (
		id         INTEGER PRIMARY KEY AUTOINCREMENT,
		scraped_at TEXT    NOT NULL,
		name       TEXT    NOT NULL,
		price      TEXT    NOT NULL
	);`

	if _, err := db.Exec(createTable); err != nil {
		db.Close()
		return nil, err
	}

	return db, nil
}

// insertPrice writes one CommodityPrice row into the database.
// Using a prepared statement parameter (?) prevents SQL injection and avoids
// re-parsing the query on every insert, which saves CPU time.
func insertPrice(db *sql.DB, cp CommodityPrice) error {
	// RFC3339 gives a human-readable, sortable timestamp string.
	_, err := db.Exec(
		`INSERT INTO commodity_prices (scraped_at, name, price) VALUES (?, ?, ?)`,
		cp.Timestamp.UTC().Format(time.RFC3339),
		cp.Name,
		cp.Price,
	)
	return err
}

// ---------------------------------------------------------------------------
// Scraper
// ---------------------------------------------------------------------------

// newCollector builds a configured Colly collector ready for scraping.
func newCollector() *colly.Collector {
	c := colly.NewCollector(
		// Limit the response body size to 10 MB so a large page never exhausts
		// available RAM on a 1 GB VPS.
		colly.MaxBodySize(10*1024*1024),
	)

	// Restrict concurrency to a single goroutine and apply a random delay
	// between MinDelaySec and MaxDelaySec seconds on every request.
	// Colly's Delay adds a fixed base wait and RandomDelay adds a uniform
	// random component in [0, RandomDelay], giving an effective range of
	// [MinDelaySec, MaxDelaySec].  This is memory-efficient and polite to the
	// target server.
	if err := c.Limit(&colly.LimitRule{
		DomainGlob:  "*",
		Parallelism: 1,
		Delay:       time.Duration(MinDelaySec) * time.Second,
		RandomDelay: time.Duration(MaxDelaySec-MinDelaySec) * time.Second,
	}); err != nil {
		log.Fatalf("failed to set rate limit: %v", err)
	}

	// Rotate through the User-Agent pool before each request to reduce the
	// chance of being identified as an automated client.
	c.OnRequest(func(r *colly.Request) {
		ua := userAgents[rng.Intn(len(userAgents))]
		r.Headers.Set("User-Agent", ua)

		log.Printf("visiting %s  (User-Agent: %.40s…)", r.URL, ua)
	})

	// Log any HTTP-level errors so they surface clearly in the VPS logs.
	c.OnError(func(r *colly.Response, err error) {
		log.Printf("request error: %s – %v (HTTP %d)", r.Request.URL, err, r.StatusCode)
	})

	return c
}

// runScraper registers commodity-parsing callbacks on c and visits each URL in
// urls sequentially.  State (nameBuffer, scrapeTime) is reset at the start of
// every request so multiple URLs do not bleed into each other.
//
// A fresh collector (from newCollector) should be passed on each call so that
// callbacks are only registered once per collector instance.
func runScraper(c *colly.Collector, db *sql.DB, urls []string) {
	// nameBuffer temporarily holds the commodity name from NameSelector so it
	// can be paired with the next PriceSelector element.
	//
	// scrapeTime is reset per-request so each URL's rows carry the timestamp of
	// when that specific page was fetched.
	//
	// NOTE: This simple pairing assumes the HTML layout alternates name/price
	// cells in document order (e.g. a two-column table).  Adjust the selectors
	// and pairing logic if the target page has a different structure.
	var (
		nameBuffer string
		scrapeTime time.Time
	)

	// Reset per-URL state at the beginning of each request so leftover values
	// from a previous URL do not contaminate the next one.
	// Access to nameBuffer and scrapeTime is safe without a mutex because
	// Parallelism is set to 1 in newCollector, ensuring all Colly callbacks
	// execute sequentially on a single goroutine.
	c.OnRequest(func(r *colly.Request) {
		scrapeTime = time.Now()
		nameBuffer = ""
	})

	// OnHTML fires for every element matching NameSelector.
	// We save the text content so the paired price handler can use it.
	c.OnHTML(NameSelector, func(e *colly.HTMLElement) {
		nameBuffer = e.Text
	})

	// OnHTML fires for every element matching PriceSelector.
	// We pair it with the most recently buffered name and write to SQLite.
	c.OnHTML(PriceSelector, func(e *colly.HTMLElement) {
		price := e.Text
		if nameBuffer == "" || price == "" {
			// Skip incomplete pairs (e.g. header row or missing data).
			return
		}

		cp := CommodityPrice{
			Timestamp: scrapeTime,
			Name:      nameBuffer,
			Price:     price,
		}

		// Write the row to SQLite immediately rather than accumulating a slice,
		// so memory usage stays O(1) regardless of how many rows are on the page.
		if err := insertPrice(db, cp); err != nil {
			log.Printf("db insert error for %q: %v", cp.Name, err)
			return
		}

		log.Printf("saved: %s = %s", cp.Name, cp.Price)

		// Reset the buffer so a stray price element doesn't re-use an old name.
		nameBuffer = ""
	})

	// OnScraped fires after each page has been fully processed.
	c.OnScraped(func(r *colly.Response) {
		log.Printf("finished scraping %s", r.Request.URL)
	})

	// Visit each configured URL in turn.  Errors are logged but do not stop
	// the remaining URLs from being scraped.
	for _, u := range urls {
		if err := c.Visit(u); err != nil {
			log.Printf("visit error for %s: %v", u, err)
		}
	}
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

func main() {
	// Seed the package-level random source used for User-Agent rotation.
	// math/rand is sufficient; no cryptographic randomness is needed here.
	rng = rand.New(rand.NewSource(time.Now().UnixNano())) //nolint:gosec

	// Open (or create) the SQLite database and ensure the schema exists.
	db, err := openDB(DBFile)
	if err != nil {
		log.Fatalf("cannot open database %q: %v", DBFile, err)
	}
	defer db.Close()

	log.Printf("database ready: %s", DBFile)

	// Determine which URLs to scrape and how often to repeat.
	urls := resolveURLs()
	interval := resolveInterval()

	// runCycle performs one full scrape of all configured URLs using a fresh
	// Colly collector.  Creating a new collector each cycle ensures that
	// OnHTML/OnRequest callbacks are registered exactly once per run and that
	// no state leaks between cycles.
	runCycle := func() {
		log.Printf("starting scrape cycle (%d URL(s))…", len(urls))
		c := newCollector()
		runScraper(c, db, urls)
		log.Println("scrape cycle complete")
	}

	// Always run once immediately on startup so there is no initial wait.
	runCycle()

	// If interval is 0, run-once mode: exit after the first cycle.
	// This is useful when an external scheduler (e.g. cron, systemd timer)
	// manages the cadence instead of the built-in scheduler.
	if interval == 0 {
		log.Println("SCRAPER_INTERVAL=0 – run-once mode, exiting")
		return
	}

	// Built-in scheduler: repeat every interval until SIGINT or SIGTERM.
	log.Printf("scheduler active – repeating every %v (send SIGINT/SIGTERM to stop)", interval)

	// Listen for OS shutdown signals so the process exits cleanly when the VPS
	// operator runs `systemctl stop scraper` or presses Ctrl+C.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Interval elapsed – run the next scrape cycle.
			runCycle()
			log.Printf("next run scheduled at %s", time.Now().Add(interval).Format(time.RFC3339))

		case sig := <-stop:
			// Shutdown signal received – stop the scheduler gracefully.
			log.Printf("received signal %v – stopping scheduler", sig)
			return
		}
	}
}

