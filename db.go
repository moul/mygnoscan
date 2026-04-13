package main

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"

	_ "modernc.org/sqlite"
)

type DB struct {
	db *sql.DB
	mu sync.RWMutex
}

func NewDB(path string) (*DB, error) {
	db, err := sql.Open("sqlite", path+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, err
	}

	// Migrate: drop tables if they lack UNIQUE constraints (old schema)
	var callSQL string
	db.QueryRow(`SELECT sql FROM sqlite_master WHERE type='table' AND name='calls'`).Scan(&callSQL)
	if callSQL != "" && !strings.Contains(callSQL, "UNIQUE") {
		db.Exec(`DROP TABLE IF EXISTS calls`)
		db.Exec(`DROP TABLE IF EXISTS msg_runs`)
		db.Exec(`DROP TABLE IF EXISTS bank_sends`)
	}

	// Migrate: add network column if missing (packages needs table rebuild for PK change)
	var pkgSQL string
	db.QueryRow(`SELECT sql FROM sqlite_master WHERE type='table' AND name='packages'`).Scan(&pkgSQL)
	if pkgSQL != "" && !strings.Contains(pkgSQL, "network") {
		if err := migrateAddNetworkColumn(db); err != nil {
			db.Close()
			return nil, fmt.Errorf("migrate network: %w", err)
		}
	}

	if err := initSchema(db); err != nil {
		db.Close()
		return nil, err
	}

	return &DB{db: db}, nil
}

func migrateAddNetworkColumn(db *sql.DB) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Rebuild packages with (network, path) PK
	if _, err := tx.Exec(`
		CREATE TABLE IF NOT EXISTS packages_new (
			network TEXT NOT NULL DEFAULT 'gnoland1',
			path TEXT NOT NULL,
			name TEXT NOT NULL,
			creator TEXT NOT NULL,
			block_height INTEGER NOT NULL,
			tx_hash TEXT NOT NULL,
			is_realm BOOLEAN NOT NULL,
			num_files INTEGER NOT NULL,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (network, path)
		)
	`); err != nil {
		return err
	}
	if _, err := tx.Exec(`INSERT INTO packages_new (network, path, name, creator, block_height, tx_hash, is_realm, num_files, created_at) SELECT 'gnoland1', path, name, creator, block_height, tx_hash, is_realm, num_files, created_at FROM packages`); err != nil {
		return err
	}
	if _, err := tx.Exec(`DROP TABLE packages`); err != nil {
		return err
	}
	if _, err := tx.Exec(`ALTER TABLE packages_new RENAME TO packages`); err != nil {
		return err
	}

	// Rebuild package_files with (network, package_path, file_name) PK
	if _, err := tx.Exec(`
		CREATE TABLE IF NOT EXISTS package_files_new (
			network TEXT NOT NULL DEFAULT 'gnoland1',
			package_path TEXT NOT NULL,
			file_name TEXT NOT NULL,
			body TEXT NOT NULL,
			PRIMARY KEY (network, package_path, file_name)
		)
	`); err != nil {
		return err
	}
	if _, err := tx.Exec(`INSERT INTO package_files_new (network, package_path, file_name, body) SELECT 'gnoland1', package_path, file_name, body FROM package_files`); err != nil {
		return err
	}
	if _, err := tx.Exec(`DROP TABLE package_files`); err != nil {
		return err
	}
	if _, err := tx.Exec(`ALTER TABLE package_files_new RENAME TO package_files`); err != nil {
		return err
	}

	// Rebuild dependencies with (network, package_path, import_path) PK
	if _, err := tx.Exec(`
		CREATE TABLE IF NOT EXISTS dependencies_new (
			network TEXT NOT NULL DEFAULT 'gnoland1',
			package_path TEXT NOT NULL,
			import_path TEXT NOT NULL,
			PRIMARY KEY (network, package_path, import_path)
		)
	`); err != nil {
		return err
	}
	if _, err := tx.Exec(`INSERT INTO dependencies_new (network, package_path, import_path) SELECT 'gnoland1', package_path, import_path FROM dependencies`); err != nil {
		return err
	}
	if _, err := tx.Exec(`DROP TABLE dependencies`); err != nil {
		return err
	}
	if _, err := tx.Exec(`ALTER TABLE dependencies_new RENAME TO dependencies`); err != nil {
		return err
	}

	// For calls/msg_runs/bank_sends: just add column (ignore errors if already exists)
	tx.Exec(`ALTER TABLE calls ADD COLUMN network TEXT NOT NULL DEFAULT 'gnoland1'`)
	tx.Exec(`ALTER TABLE msg_runs ADD COLUMN network TEXT NOT NULL DEFAULT 'gnoland1'`)
	tx.Exec(`ALTER TABLE bank_sends ADD COLUMN network TEXT NOT NULL DEFAULT 'gnoland1'`)

	return tx.Commit()
}

func initSchema(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS packages (
			network TEXT NOT NULL DEFAULT 'gnoland1',
			path TEXT NOT NULL,
			name TEXT NOT NULL,
			creator TEXT NOT NULL,
			block_height INTEGER NOT NULL,
			tx_hash TEXT NOT NULL,
			is_realm BOOLEAN NOT NULL,
			num_files INTEGER NOT NULL,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (network, path)
		);

		CREATE TABLE IF NOT EXISTS package_files (
			network TEXT NOT NULL DEFAULT 'gnoland1',
			package_path TEXT NOT NULL,
			file_name TEXT NOT NULL,
			body TEXT NOT NULL,
			PRIMARY KEY (network, package_path, file_name)
		);

		CREATE TABLE IF NOT EXISTS dependencies (
			network TEXT NOT NULL DEFAULT 'gnoland1',
			package_path TEXT NOT NULL,
			import_path TEXT NOT NULL,
			PRIMARY KEY (network, package_path, import_path)
		);

		CREATE TABLE IF NOT EXISTS calls (
			network TEXT NOT NULL DEFAULT 'gnoland1',
			tx_hash TEXT NOT NULL,
			block_height INTEGER NOT NULL,
			caller TEXT NOT NULL,
			pkg_path TEXT NOT NULL,
			func_name TEXT NOT NULL,
			success BOOLEAN NOT NULL,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			UNIQUE(network, tx_hash, pkg_path, func_name)
		);

		CREATE TABLE IF NOT EXISTS msg_runs (
			network TEXT NOT NULL DEFAULT 'gnoland1',
			tx_hash TEXT NOT NULL,
			block_height INTEGER NOT NULL,
			caller TEXT NOT NULL,
			source TEXT NOT NULL,
			success BOOLEAN NOT NULL,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			UNIQUE(network, tx_hash, caller)
		);

		CREATE TABLE IF NOT EXISTS bank_sends (
			network TEXT NOT NULL DEFAULT 'gnoland1',
			tx_hash TEXT NOT NULL,
			block_height INTEGER NOT NULL,
			from_address TEXT NOT NULL,
			to_address TEXT NOT NULL,
			amount TEXT NOT NULL,
			success BOOLEAN NOT NULL,
			UNIQUE(network, tx_hash, from_address, to_address)
		);

		CREATE TABLE IF NOT EXISTS sync_state (
			key TEXT PRIMARY KEY,
			value TEXT NOT NULL
		);

		CREATE INDEX IF NOT EXISTS idx_calls_pkg ON calls(pkg_path);
		CREATE INDEX IF NOT EXISTS idx_calls_caller ON calls(caller);
		CREATE INDEX IF NOT EXISTS idx_deps_import ON dependencies(import_path);
		CREATE INDEX IF NOT EXISTS idx_packages_creator ON packages(creator);
		CREATE INDEX IF NOT EXISTS idx_packages_realm ON packages(is_realm);
		CREATE INDEX IF NOT EXISTS idx_msg_runs_caller ON msg_runs(caller);
		CREATE INDEX IF NOT EXISTS idx_bank_from ON bank_sends(from_address);
		CREATE INDEX IF NOT EXISTS idx_bank_to ON bank_sends(to_address);
	`)
	return err
}

func (d *DB) Close() error {
	return d.db.Close()
}

// networkWhere returns a WHERE or AND clause for network filtering.
// If network is empty, returns empty string and no args.
func networkWhere(network string, hasWhere bool) (string, []any) {
	if network == "" {
		return "", nil
	}
	if hasWhere {
		return " AND network = ?", []any{network}
	}
	return " WHERE network = ?", []any{network}
}

// UpsertPackage inserts or updates a package.
func (d *DB) UpsertPackage(network, path, name, creator, txHash string, blockHeight int, isRealm bool, numFiles int) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, err := d.db.Exec(`
		INSERT OR REPLACE INTO packages (network, path, name, creator, tx_hash, block_height, is_realm, num_files)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, network, path, name, creator, txHash, blockHeight, isRealm, numFiles)
	return err
}

// UpsertPackageFile inserts or updates a package file.
func (d *DB) UpsertPackageFile(network, pkgPath, fileName, body string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, err := d.db.Exec(`
		INSERT OR REPLACE INTO package_files (network, package_path, file_name, body)
		VALUES (?, ?, ?, ?)
	`, network, pkgPath, fileName, body)
	return err
}

// SetDependencies replaces all dependencies for a package.
func (d *DB) SetDependencies(network, pkgPath string, imports []string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.Exec(`DELETE FROM dependencies WHERE network = ? AND package_path = ?`, network, pkgPath); err != nil {
		return err
	}

	stmt, err := tx.Prepare(`INSERT OR IGNORE INTO dependencies (network, package_path, import_path) VALUES (?, ?, ?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, imp := range imports {
		if _, err := stmt.Exec(network, pkgPath, imp); err != nil {
			return err
		}
	}

	return tx.Commit()
}

// InsertCall records a MsgCall.
func (d *DB) InsertCall(network, txHash string, blockHeight int, caller, pkgPath, funcName string, success bool) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, err := d.db.Exec(`
		INSERT OR IGNORE INTO calls (network, tx_hash, block_height, caller, pkg_path, func_name, success)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`, network, txHash, blockHeight, caller, pkgPath, funcName, success)
	return err
}

// InsertMsgRun records a MsgRun transaction with its source.
func (d *DB) InsertMsgRun(network, txHash string, blockHeight int, caller, source string, success bool) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, err := d.db.Exec(`
		INSERT OR IGNORE INTO msg_runs (network, tx_hash, block_height, caller, source, success)
		VALUES (?, ?, ?, ?, ?, ?)
	`, network, txHash, blockHeight, caller, source, success)
	return err
}

func (d *DB) InsertBankSend(network, txHash string, blockHeight int, from, to, amount string, success bool) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, err := d.db.Exec(`INSERT OR IGNORE INTO bank_sends (network, tx_hash, block_height, from_address, to_address, amount, success) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		network, txHash, blockHeight, from, to, amount, success)
	return err
}

// GetSyncState reads a sync state value.
func (d *DB) GetSyncState(key string) (string, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	var val string
	err := d.db.QueryRow(`SELECT value FROM sync_state WHERE key = ?`, key).Scan(&val)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return val, err
}

// SetSyncState writes a sync state value.
func (d *DB) SetSyncState(key, value string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, err := d.db.Exec(`
		INSERT OR REPLACE INTO sync_state (key, value) VALUES (?, ?)
	`, key, value)
	return err
}

type PackageInfo struct {
	Network     string `json:"network,omitempty"`
	Path        string `json:"path"`
	Name        string `json:"name"`
	Creator     string `json:"creator"`
	BlockHeight int    `json:"block_height"`
	BlockTime   string `json:"block_time,omitempty"`
	TxHash      string `json:"tx_hash"`
	IsRealm     bool   `json:"is_realm"`
	NumFiles    int    `json:"num_files"`
}

type PackageDetail struct {
	PackageInfo
	Files      []FileInfo   `json:"files"`
	Imports    []string     `json:"imports"`
	Dependents []string     `json:"dependents"`
	Callers    []CallInfo   `json:"recent_calls"`
	MsgRunRefs []MsgRunInfo `json:"msgrun_refs"`
	CallCount  int          `json:"call_count"`
}

type FileInfo struct {
	Name string `json:"name"`
	Body string `json:"body"`
}

type CallInfo struct {
	TxHash      string `json:"tx_hash"`
	BlockHeight int    `json:"block_height"`
	Caller      string `json:"caller"`
	FuncName    string `json:"func_name"`
	Success     bool   `json:"success"`
}

type MsgRunInfo struct {
	TxHash      string `json:"tx_hash"`
	BlockHeight int    `json:"block_height"`
	Caller      string `json:"caller"`
	Success     bool   `json:"success"`
}

func (d *DB) CountPackages(network string, realmOnly bool) (int, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	q := `SELECT COUNT(*) FROM packages WHERE is_realm = ?`
	args := []any{realmOnly}
	if network != "" {
		q += ` AND network = ?`
		args = append(args, network)
	}
	var count int
	err := d.db.QueryRow(q, args...).Scan(&count)
	return count, err
}

// ListPackages returns all packages, optionally filtered.
func (d *DB) ListPackages(network string, realmOnly bool, limit, offset int) ([]PackageInfo, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	q := `SELECT network, path, name, creator, block_height, tx_hash, is_realm, num_files FROM packages WHERE is_realm = ?`
	args := []any{realmOnly}
	if network != "" {
		q += ` AND network = ?`
		args = append(args, network)
	}
	q += ` ORDER BY block_height DESC`
	if limit > 0 {
		q += fmt.Sprintf(` LIMIT %d OFFSET %d`, limit, offset)
	}

	rows, err := d.db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var pkgs []PackageInfo
	for rows.Next() {
		var p PackageInfo
		if err := rows.Scan(&p.Network, &p.Path, &p.Name, &p.Creator, &p.BlockHeight, &p.TxHash, &p.IsRealm, &p.NumFiles); err != nil {
			return nil, err
		}
		pkgs = append(pkgs, p)
	}
	return pkgs, rows.Err()
}

// GetPackageDetail returns full details for a package.
func (d *DB) GetPackageDetail(network, path string) (*PackageDetail, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	q := `SELECT network, path, name, creator, block_height, tx_hash, is_realm, num_files FROM packages WHERE path = ?`
	args := []any{path}
	if network != "" {
		q += ` AND network = ?`
		args = append(args, network)
	}

	var p PackageDetail
	err := d.db.QueryRow(q, args...).Scan(&p.Network, &p.Path, &p.Name, &p.Creator, &p.BlockHeight, &p.TxHash, &p.IsRealm, &p.NumFiles)
	if err != nil {
		return nil, err
	}

	// Files
	filesQ := `SELECT file_name, body FROM package_files WHERE package_path = ? AND network = ?`
	rows, err := d.db.Query(filesQ, path, p.Network)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var f FileInfo
		if err := rows.Scan(&f.Name, &f.Body); err != nil {
			return nil, err
		}
		p.Files = append(p.Files, f)
	}

	// Imports (dependencies)
	impRows, err := d.db.Query(`SELECT import_path FROM dependencies WHERE package_path = ? AND network = ?`, path, p.Network)
	if err != nil {
		return nil, err
	}
	defer impRows.Close()
	for impRows.Next() {
		var imp string
		if err := impRows.Scan(&imp); err != nil {
			return nil, err
		}
		p.Imports = append(p.Imports, imp)
	}

	// Dependents (who imports this)
	depRows, err := d.db.Query(`SELECT package_path FROM dependencies WHERE import_path = ? AND network = ?`, path, p.Network)
	if err != nil {
		return nil, err
	}
	defer depRows.Close()
	for depRows.Next() {
		var dep string
		if err := depRows.Scan(&dep); err != nil {
			return nil, err
		}
		p.Dependents = append(p.Dependents, dep)
	}

	// Recent calls
	callRows, err := d.db.Query(`
		SELECT tx_hash, block_height, caller, func_name, success
		FROM calls WHERE pkg_path = ? AND network = ?
		ORDER BY block_height DESC LIMIT 50
	`, path, p.Network)
	if err != nil {
		return nil, err
	}
	defer callRows.Close()
	for callRows.Next() {
		var c CallInfo
		if err := callRows.Scan(&c.TxHash, &c.BlockHeight, &c.Caller, &c.FuncName, &c.Success); err != nil {
			return nil, err
		}
		p.Callers = append(p.Callers, c)
	}

	// Call count
	d.db.QueryRow(`SELECT COUNT(*) FROM calls WHERE pkg_path = ? AND network = ?`, path, p.Network).Scan(&p.CallCount)

	// MsgRun references (where source contains import of this path)
	runRows, err := d.db.Query(`
		SELECT tx_hash, block_height, caller, success
		FROM msg_runs WHERE source LIKE ? AND network = ?
		ORDER BY block_height DESC LIMIT 50
	`, "%"+path+"%", p.Network)
	if err != nil {
		return nil, err
	}
	defer runRows.Close()
	for runRows.Next() {
		var r MsgRunInfo
		if err := runRows.Scan(&r.TxHash, &r.BlockHeight, &r.Caller, &r.Success); err != nil {
			return nil, err
		}
		p.MsgRunRefs = append(p.MsgRunRefs, r)
	}

	return &p, nil
}

// GetDependencyGraph returns the full dependency graph for a package (recursive).
func (d *DB) GetDependencyGraph(network, path string) (map[string][]string, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	graph := make(map[string][]string)
	visited := make(map[string]bool)

	var walk func(p string) error
	walk = func(p string) error {
		if visited[p] {
			return nil
		}
		visited[p] = true

		var rows *sql.Rows
		var err error
		if network != "" {
			rows, err = d.db.Query(`SELECT import_path FROM dependencies WHERE package_path = ? AND network = ?`, p, network)
		} else {
			rows, err = d.db.Query(`SELECT import_path FROM dependencies WHERE package_path = ?`, p)
		}
		if err != nil {
			return err
		}
		defer rows.Close()

		var deps []string
		for rows.Next() {
			var dep string
			if err := rows.Scan(&dep); err != nil {
				return err
			}
			deps = append(deps, dep)
		}
		graph[p] = deps

		for _, dep := range deps {
			if strings.HasPrefix(dep, "gno.land/") {
				if err := walk(dep); err != nil {
					return err
				}
			}
		}
		return nil
	}

	if err := walk(path); err != nil {
		return nil, err
	}
	return graph, nil
}

// GetReverseGraph returns all packages that depend on path (recursive).
func (d *DB) GetReverseGraph(network, path string) (map[string][]string, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	graph := make(map[string][]string)
	visited := make(map[string]bool)

	var walk func(p string) error
	walk = func(p string) error {
		if visited[p] {
			return nil
		}
		visited[p] = true

		var rows *sql.Rows
		var err error
		if network != "" {
			rows, err = d.db.Query(`SELECT package_path FROM dependencies WHERE import_path = ? AND network = ?`, p, network)
		} else {
			rows, err = d.db.Query(`SELECT package_path FROM dependencies WHERE import_path = ?`, p)
		}
		if err != nil {
			return err
		}
		defer rows.Close()

		var deps []string
		for rows.Next() {
			var dep string
			if err := rows.Scan(&dep); err != nil {
				return err
			}
			deps = append(deps, dep)
		}
		graph[p] = deps

		for _, dep := range deps {
			if err := walk(dep); err != nil {
				return err
			}
		}
		return nil
	}

	if err := walk(path); err != nil {
		return nil, err
	}
	return graph, nil
}

type Stats struct {
	TotalTxs      int `json:"total_txs"`
	TotalCalls    int `json:"total_calls"`
	TotalDeploys  int `json:"total_deploys"`
	TotalMsgRuns  int `json:"total_msg_runs"`
	TotalSends    int `json:"total_sends"`
	TotalRealms   int `json:"total_realms"`
	TotalPackages int `json:"total_packages"`
	UniqueCallers int `json:"unique_callers"`
	LatestBlock   int `json:"latest_block"`
}

// GetStats returns aggregate statistics.
func (d *DB) GetStats(network string) (*Stats, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	var s Stats
	nf := ""
	if network != "" {
		nf = " WHERE network = '" + strings.ReplaceAll(network, "'", "''") + "'"
	}
	d.db.QueryRow(`SELECT COUNT(*) FROM calls` + nf).Scan(&s.TotalCalls)
	d.db.QueryRow(`SELECT COUNT(*) FROM packages` + nf).Scan(&s.TotalDeploys)
	if network != "" {
		d.db.QueryRow(`SELECT COUNT(*) FROM packages WHERE is_realm = 1 AND network = ?`, network).Scan(&s.TotalRealms)
	} else {
		d.db.QueryRow(`SELECT COUNT(*) FROM packages WHERE is_realm = 1`).Scan(&s.TotalRealms)
	}
	s.TotalPackages = s.TotalDeploys - s.TotalRealms
	d.db.QueryRow(`SELECT COUNT(*) FROM msg_runs` + nf).Scan(&s.TotalMsgRuns)
	d.db.QueryRow(`SELECT COUNT(*) FROM bank_sends` + nf).Scan(&s.TotalSends)
	s.TotalTxs = s.TotalCalls + s.TotalDeploys + s.TotalMsgRuns + s.TotalSends
	if network != "" {
		d.db.QueryRow(`SELECT COUNT(DISTINCT caller) FROM calls WHERE network = ?`, network).Scan(&s.UniqueCallers)
		d.db.QueryRow(`SELECT COALESCE(MAX(block_height), 0) FROM packages WHERE network = ?`, network).Scan(&s.LatestBlock)
	} else {
		d.db.QueryRow(`SELECT COUNT(DISTINCT caller) FROM calls`).Scan(&s.UniqueCallers)
		d.db.QueryRow(`SELECT COALESCE(MAX(block_height), 0) FROM packages`).Scan(&s.LatestBlock)
	}
	return &s, nil
}

// Search searches across packages and callers.
func (d *DB) Search(network, q string) ([]PackageInfo, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	qStr := `
		SELECT network, path, name, creator, block_height, tx_hash, is_realm, num_files
		FROM packages
		WHERE (path LIKE ? OR name LIKE ? OR creator LIKE ?)`
	args := []any{"%" + q + "%", "%" + q + "%", "%" + q + "%"}
	if network != "" {
		qStr += ` AND network = ?`
		args = append(args, network)
	}
	qStr += ` ORDER BY block_height DESC LIMIT 20`

	rows, err := d.db.Query(qStr, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var pkgs []PackageInfo
	for rows.Next() {
		var p PackageInfo
		if err := rows.Scan(&p.Network, &p.Path, &p.Name, &p.Creator, &p.BlockHeight, &p.TxHash, &p.IsRealm, &p.NumFiles); err != nil {
			return nil, err
		}
		pkgs = append(pkgs, p)
	}
	return pkgs, rows.Err()
}

type TokenInfo struct {
	Path      string `json:"path"`
	Name      string `json:"name"`
	Creator   string `json:"creator"`
	CallCount int    `json:"call_count"`
}

func (d *DB) GetTokenPackages(network string) ([]TokenInfo, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	q := `
		SELECT DISTINCT p.path, p.name, p.creator, COALESCE(c.cnt, 0)
		FROM packages p
		JOIN dependencies dep ON dep.package_path = p.path AND dep.network = p.network
		LEFT JOIN (SELECT pkg_path, network, COUNT(*) as cnt FROM calls GROUP BY pkg_path, network) c ON c.pkg_path = p.path AND c.network = p.network
		WHERE dep.import_path LIKE '%grc20%'`
	args := []any{}
	if network != "" {
		q += ` AND p.network = ?`
		args = append(args, network)
	}
	q += ` ORDER BY p.block_height DESC`
	rows, err := d.db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var tokens []TokenInfo
	for rows.Next() {
		var t TokenInfo
		if err := rows.Scan(&t.Path, &t.Name, &t.Creator, &t.CallCount); err != nil {
			return nil, err
		}
		tokens = append(tokens, t)
	}
	return tokens, rows.Err()
}

type AccountInfo struct {
	Address     string `json:"address"`
	CallCount   int    `json:"call_count"`
	DeployCount int    `json:"deploy_count"`
	MsgRunCount int    `json:"msgrun_count"`
	SendCount   int    `json:"send_count"`
}

func (d *DB) GetActiveAccounts(network string) ([]AccountInfo, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	nFilter := ""
	if network != "" {
		// SQLite-safe string interpolation for CTEs
		safe := strings.ReplaceAll(network, "'", "''")
		nFilter = " WHERE network = '" + safe + "'"
	}

	q := `
		SELECT address, SUM(call_count), SUM(deploy_count), SUM(run_count), SUM(send_count)
		FROM (
			SELECT caller as address, COUNT(*) as call_count, 0 as deploy_count, 0 as run_count, 0 as send_count FROM calls` + nFilter + ` GROUP BY caller
			UNION ALL
			SELECT creator as address, 0, COUNT(*), 0, 0 FROM packages` + nFilter + ` GROUP BY creator
			UNION ALL
			SELECT caller as address, 0, 0, COUNT(*), 0 FROM msg_runs` + nFilter + ` GROUP BY caller
			UNION ALL
			SELECT from_address as address, 0, 0, 0, COUNT(*) FROM bank_sends` + nFilter + ` GROUP BY from_address
			UNION ALL
			SELECT to_address as address, 0, 0, 0, COUNT(*) FROM bank_sends` + nFilter + ` GROUP BY to_address
		)
		GROUP BY address
		ORDER BY (SUM(call_count) + SUM(deploy_count) + SUM(run_count) + SUM(send_count)) DESC
		LIMIT 100
	`
	rows, err := d.db.Query(q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var accounts []AccountInfo
	for rows.Next() {
		var a AccountInfo
		if err := rows.Scan(&a.Address, &a.CallCount, &a.DeployCount, &a.MsgRunCount, &a.SendCount); err != nil {
			return nil, err
		}
		accounts = append(accounts, a)
	}
	return accounts, rows.Err()
}

func (d *DB) TotalSourceBytes(network string) int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	var n int
	if network != "" {
		d.db.QueryRow(`SELECT COALESCE(SUM(LENGTH(body)), 0) FROM package_files WHERE network = ?`, network).Scan(&n)
	} else {
		d.db.QueryRow(`SELECT COALESCE(SUM(LENGTH(body)), 0) FROM package_files`).Scan(&n)
	}
	return n
}

type AddrStat struct {
	Address string `json:"address"`
	Count   int    `json:"count"`
	Total   int64  `json:"total"`
}

type BankStats struct {
	TotalSends      int        `json:"total_sends"`
	UniqueSenders   int        `json:"unique_senders"`
	UniqueReceivers int        `json:"unique_receivers"`
	UniqueAddresses int        `json:"unique_addresses"`
	TotalVolume     int64      `json:"total_volume"`
	TopSenders      []AddrStat `json:"top_senders"`
	TopReceiversVol []AddrStat `json:"top_receivers_volume"`
	TopReceiversCnt []AddrStat `json:"top_receivers_count"`
}

const amountExpr = `COALESCE(SUM(CAST(REPLACE(REPLACE(amount, 'ugnot', ''), '"', '') AS INTEGER)), 0)`

func (d *DB) GetBankStats(network string) (*BankStats, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	nFilter := ""
	if network != "" {
		safe := strings.ReplaceAll(network, "'", "''")
		nFilter = " WHERE network = '" + safe + "'"
	}

	var s BankStats
	d.db.QueryRow(`SELECT COUNT(*) FROM bank_sends` + nFilter).Scan(&s.TotalSends)
	d.db.QueryRow(`SELECT COUNT(DISTINCT from_address) FROM bank_sends` + nFilter).Scan(&s.UniqueSenders)
	d.db.QueryRow(`SELECT COUNT(DISTINCT to_address) FROM bank_sends` + nFilter).Scan(&s.UniqueReceivers)
	d.db.QueryRow(`SELECT ` + amountExpr + ` FROM bank_sends` + nFilter).Scan(&s.TotalVolume)

	andFilter := ""
	if network != "" {
		safe := strings.ReplaceAll(network, "'", "''")
		andFilter = " AND network = '" + safe + "'"
	}
	d.db.QueryRow(`SELECT COUNT(DISTINCT addr) FROM (SELECT from_address as addr FROM bank_sends` + nFilter + ` UNION SELECT to_address FROM bank_sends` + nFilter + `)`).Scan(&s.UniqueAddresses)

	s.TopSenders = d.queryAddrStats(`SELECT from_address, COUNT(*), ` + amountExpr + ` FROM bank_sends` + nFilter + ` GROUP BY from_address ORDER BY COUNT(*) DESC LIMIT 10`)
	s.TopReceiversVol = d.queryAddrStats(`SELECT to_address, COUNT(*), ` + amountExpr + ` FROM bank_sends` + nFilter + ` GROUP BY to_address ORDER BY ` + amountExpr + ` DESC LIMIT 10`)
	s.TopReceiversCnt = d.queryAddrStats(`SELECT to_address, COUNT(*), ` + amountExpr + ` FROM bank_sends` + nFilter + ` GROUP BY to_address ORDER BY COUNT(*) DESC LIMIT 10`)

	_ = andFilter
	return &s, nil
}

type RealmActivity struct {
	Path       string `json:"path"`
	Calls      int    `json:"calls"`
	Callers    int    `json:"callers"`
	Dependents int    `json:"dependents"`
	IsRealm    bool   `json:"is_realm"`
}

type CallerActivity struct {
	Address string `json:"address"`
	Calls   int    `json:"calls"`
	Realms  int    `json:"realms"`
}

type ImportRank struct {
	Path    string `json:"path"`
	Imports int    `json:"imports"`
}

type Analytics struct {
	// Summaries
	TotalRealms    int `json:"total_realms"`
	TotalPackages  int `json:"total_packages"`
	TotalCalls     int `json:"total_calls"`
	TotalDeploys   int `json:"total_deploys"`
	TotalMsgRuns   int `json:"total_msg_runs"`
	TotalSends     int `json:"total_sends"`
	TotalAddresses int `json:"total_addresses"`
	TotalSourceKB  int `json:"total_source_kb"`

	// Rankings
	TopRealms    []RealmActivity  `json:"top_realms"`
	TopPackages  []RealmActivity  `json:"top_packages"`
	TopCallers   []CallerActivity `json:"top_callers"`
	TopImports   []ImportRank     `json:"top_imports"`
	TopDeployers []CallerActivity `json:"top_deployers"`
	RecentRealms []PackageInfo    `json:"recent_realms"`
}

func (d *DB) GetAnalytics(network string) (*Analytics, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	nFilter := ""
	pFilter := ""
	if network != "" {
		safe := strings.ReplaceAll(network, "'", "''")
		nFilter = " WHERE network = '" + safe + "'"
		pFilter = " AND p.network = '" + safe + "'"
	}

	var a Analytics
	if network != "" {
		d.db.QueryRow(`SELECT COUNT(*) FROM packages WHERE is_realm = 1 AND network = ?`, network).Scan(&a.TotalRealms)
		d.db.QueryRow(`SELECT COUNT(*) FROM packages WHERE is_realm = 0 AND network = ?`, network).Scan(&a.TotalPackages)
	} else {
		d.db.QueryRow(`SELECT COUNT(*) FROM packages WHERE is_realm = 1`).Scan(&a.TotalRealms)
		d.db.QueryRow(`SELECT COUNT(*) FROM packages WHERE is_realm = 0`).Scan(&a.TotalPackages)
	}
	d.db.QueryRow(`SELECT COUNT(*) FROM calls` + nFilter).Scan(&a.TotalCalls)
	d.db.QueryRow(`SELECT COUNT(*) FROM packages` + nFilter).Scan(&a.TotalDeploys)
	d.db.QueryRow(`SELECT COUNT(*) FROM msg_runs` + nFilter).Scan(&a.TotalMsgRuns)
	d.db.QueryRow(`SELECT COUNT(*) FROM bank_sends` + nFilter).Scan(&a.TotalSends)

	addrUnionFilter := ""
	if network != "" {
		safe := strings.ReplaceAll(network, "'", "''")
		addrUnionFilter = " WHERE network = '" + safe + "'"
	}
	d.db.QueryRow(`SELECT COUNT(DISTINCT addr) FROM (
		SELECT caller as addr FROM calls` + addrUnionFilter + ` UNION SELECT creator FROM packages` + addrUnionFilter + `
		UNION SELECT caller FROM msg_runs` + addrUnionFilter + ` UNION SELECT from_address FROM bank_sends` + addrUnionFilter + `
		UNION SELECT to_address FROM bank_sends` + addrUnionFilter + `
	)`).Scan(&a.TotalAddresses)
	if network != "" {
		d.db.QueryRow(`SELECT COALESCE(SUM(LENGTH(body)), 0) / 1024 FROM package_files WHERE network = ?`, network).Scan(&a.TotalSourceKB)
	} else {
		d.db.QueryRow(`SELECT COALESCE(SUM(LENGTH(body)), 0) / 1024 FROM package_files`).Scan(&a.TotalSourceKB)
	}

	callJoinFilter := ""
	depJoinFilter := ""
	if network != "" {
		safe := strings.ReplaceAll(network, "'", "''")
		callJoinFilter = " AND c_inner.network = '" + safe + "'"
		depJoinFilter = " AND dep_inner.network = '" + safe + "'"
	}

	// Top realms by calls
	rows, _ := d.db.Query(`
		SELECT p.path, COALESCE(c.cnt, 0), COALESCE(c.callers, 0), COALESCE(dep.cnt, 0), p.is_realm
		FROM packages p
		LEFT JOIN (SELECT pkg_path, COUNT(*) as cnt, COUNT(DISTINCT caller) as callers FROM calls GROUP BY pkg_path) c ON c.pkg_path = p.path
		LEFT JOIN (SELECT import_path, COUNT(*) as cnt FROM dependencies GROUP BY import_path) dep ON dep.import_path = p.path
		WHERE p.is_realm = 1` + pFilter + `
		ORDER BY COALESCE(c.cnt, 0) DESC LIMIT 15
	`)
	if rows != nil {
		defer rows.Close()
		for rows.Next() {
			var r RealmActivity
			rows.Scan(&r.Path, &r.Calls, &r.Callers, &r.Dependents, &r.IsRealm)
			a.TopRealms = append(a.TopRealms, r)
		}
	}

	// Top packages by imports (dependents)
	rows2, _ := d.db.Query(`
		SELECT p.path, COALESCE(c.cnt, 0), 0, COALESCE(dep.cnt, 0), p.is_realm
		FROM packages p
		LEFT JOIN (SELECT pkg_path, COUNT(*) as cnt FROM calls GROUP BY pkg_path) c ON c.pkg_path = p.path
		LEFT JOIN (SELECT import_path, COUNT(*) as cnt FROM dependencies GROUP BY import_path) dep ON dep.import_path = p.path
		WHERE p.is_realm = 0` + pFilter + `
		ORDER BY COALESCE(dep.cnt, 0) DESC LIMIT 15
	`)
	if rows2 != nil {
		defer rows2.Close()
		for rows2.Next() {
			var r RealmActivity
			rows2.Scan(&r.Path, &r.Calls, &r.Callers, &r.Dependents, &r.IsRealm)
			a.TopPackages = append(a.TopPackages, r)
		}
	}

	// Top callers
	callersQ := `SELECT caller, COUNT(*) as c, COUNT(DISTINCT pkg_path) as realms FROM calls` + nFilter + ` GROUP BY caller ORDER BY c DESC LIMIT 15`
	rows3, _ := d.db.Query(callersQ)
	if rows3 != nil {
		defer rows3.Close()
		for rows3.Next() {
			var c CallerActivity
			rows3.Scan(&c.Address, &c.Calls, &c.Realms)
			a.TopCallers = append(a.TopCallers, c)
		}
	}

	// Top imports
	importsQ := `SELECT import_path, COUNT(*) as c FROM dependencies WHERE import_path LIKE 'gno.land/%'`
	if network != "" {
		importsQ += ` AND network = '` + strings.ReplaceAll(network, "'", "''") + `'`
	}
	importsQ += ` GROUP BY import_path ORDER BY c DESC LIMIT 15`
	rows4, _ := d.db.Query(importsQ)
	if rows4 != nil {
		defer rows4.Close()
		for rows4.Next() {
			var i ImportRank
			rows4.Scan(&i.Path, &i.Imports)
			a.TopImports = append(a.TopImports, i)
		}
	}

	// Top deployers
	deployQ := `SELECT creator, COUNT(*) as c, 0 FROM packages` + nFilter + ` GROUP BY creator ORDER BY c DESC LIMIT 15`
	rows5, _ := d.db.Query(deployQ)
	if rows5 != nil {
		defer rows5.Close()
		for rows5.Next() {
			var c CallerActivity
			rows5.Scan(&c.Address, &c.Calls, &c.Realms)
			a.TopDeployers = append(a.TopDeployers, c)
		}
	}

	// Recent realms
	recentQ := `SELECT network, path, name, creator, block_height, tx_hash, is_realm, num_files FROM packages WHERE is_realm = 1` + pFilter + ` ORDER BY block_height DESC LIMIT 10`
	rows6, _ := d.db.Query(recentQ)
	if rows6 != nil {
		defer rows6.Close()
		for rows6.Next() {
			var p PackageInfo
			rows6.Scan(&p.Network, &p.Path, &p.Name, &p.Creator, &p.BlockHeight, &p.TxHash, &p.IsRealm, &p.NumFiles)
			a.RecentRealms = append(a.RecentRealms, p)
		}
	}

	_ = callJoinFilter
	_ = depJoinFilter

	return &a, nil
}

func (d *DB) queryAddrStats(query string) []AddrStat {
	rows, err := d.db.Query(query)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var result []AddrStat
	for rows.Next() {
		var s AddrStat
		if err := rows.Scan(&s.Address, &s.Count, &s.Total); err != nil {
			continue
		}
		result = append(result, s)
	}
	return result
}
