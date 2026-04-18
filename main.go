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
// Configuration is done via the constants at the top of the file.
// When the target website is known, update TargetURL, NameSelector, and
// PriceSelector accordingly.
package main

import (
	"database/sql"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/gocolly/colly/v2"
	_ "github.com/mattn/go-sqlite3" // SQLite driver registered via side-effect import
)

// ---------------------------------------------------------------------------
// Configuration – update these values to point at the real commodity site.
// ---------------------------------------------------------------------------

const (
	// TargetURL is the page that lists commodity prices.
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
)

// rng is a package-level random source used for User-Agent rotation and delay
// randomisation.  It is initialised once in main() with a time-based seed so
// the sequence differs on every run.
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

// runScraper attaches commodity-parsing callbacks to the collector and visits
// targetURL.  Each matched name/price pair is written immediately to the
// database, keeping memory usage constant regardless of page size.
func runScraper(c *colly.Collector, db *sql.DB, targetURL string) {
	// scrapeTime is set once per visit so all rows from a single run share the
	// same timestamp, making it easy to group results by scrape session.
	scrapeTime := time.Now()

	// nameBuffer temporarily stores the commodity name found in NameSelector
	// so it can be paired with the price found by PriceSelector.
	//
	// NOTE: This simple pairing assumes the HTML layout alternates name/price
	// cells in document order (e.g. a two-column table).  Adjust the selectors
	// and pairing logic if the target page has a different structure.
	var nameBuffer string

	// OnHTML fires for every element matching NameSelector.
	// We save the text content so the paired price handler can use it.
	c.OnHTML(NameSelector, func(e *colly.HTMLElement) {
		// Text() trims surrounding whitespace automatically.
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

	// OnScraped fires after the page has been fully processed.
	c.OnScraped(func(r *colly.Response) {
		log.Printf("finished scraping %s", r.Request.URL)
	})

	// Visit triggers the request; OnHTML callbacks run synchronously before
	// Visit returns, so the database is fully populated when Visit exits.
	if err := c.Visit(targetURL); err != nil {
		log.Printf("visit error: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

func main() {
	// Seed the package-level random source used for User-Agent rotation.
	// Without seeding, the same sequence repeats each run, defeating the
	// randomisation goal.  math/rand is sufficient; no cryptographic
	// randomness is needed here.
	rng = rand.New(rand.NewSource(time.Now().UnixNano())) //nolint:gosec

	// Open (or create) the SQLite database and ensure the schema exists.
	db, err := openDB(DBFile)
	if err != nil {
		log.Fatalf("cannot open database %q: %v", DBFile, err)
	}
	defer db.Close()

	log.Printf("database ready: %s", DBFile)

	// Check whether the target URL has been configured.  If it is still the
	// placeholder, warn the operator but proceed so the rest of the code can
	// be tested against a real URL supplied via an environment variable.
	targetURL := TargetURL
	if envURL := os.Getenv("SCRAPER_TARGET_URL"); envURL != "" {
		targetURL = envURL
		log.Printf("using target URL from SCRAPER_TARGET_URL: %s", targetURL)
	} else if targetURL == "https://example.com/commodities" {
		log.Println("WARNING: TargetURL is still the placeholder. " +
			"Set SCRAPER_TARGET_URL or update the TargetURL constant before running in production.")
	}

	// Build the Colly collector with rate-limiting and User-Agent rotation.
	c := newCollector()

	// Register CSS-selector callbacks and visit the target page.
	// All database writes happen inside runScraper.
	runScraper(c, db, targetURL)

	log.Println("scrape complete")
}
