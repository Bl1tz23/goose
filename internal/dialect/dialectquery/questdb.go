package dialectquery

import "fmt"

type Questdb struct{}

var _ Querier = (*Questdb)(nil)

func (p *Questdb) CreateTable(tableName string) string {
	q := `CREATE TABLE %s (
		version_id bigint NOT NULL,
		is_applied boolean NOT NULL,
		tstamp timestamp NOT NULL
	)`
	return fmt.Sprintf(q, tableName)
}

func (p *Questdb) InsertVersion(tableName string) string {
	q := `INSERT INTO %s (version_id, is_applied, tstamp) VALUES ($1, $2, now())`
	return fmt.Sprintf(q, tableName)
}

func (p *Questdb) DeleteVersion(tableName string) string {
	q := `CREATE TABLE %s_copy AS (
		SELECT * FROM %s WHERE version_id != $1
	)`
	return fmt.Sprintf(q, tableName)
}

func (p *Questdb) GetMigrationByVersion(tableName string) string {
	q := `SELECT tstamp, is_applied FROM %s WHERE version_id=$1 ORDER BY tstamp DESC LIMIT 1`
	return fmt.Sprintf(q, tableName)
}

func (p *Questdb) ListMigrations(tableName string) string {
	q := `SELECT version_id, is_applied from %s ORDER BY id DESC`
	return fmt.Sprintf(q, tableName)
}

func (p *Questdb) GetLatestVersion(tableName string) string {
	q := `SELECT max(version_id) FROM %s`
	return fmt.Sprintf(q, tableName)
}
