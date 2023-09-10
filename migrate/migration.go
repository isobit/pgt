package migrate

import (
	"fmt"
	"io/fs"
	"regexp"
	"strconv"
	"strings"
)

var (
	filenamePattern      = regexp.MustCompile(`^(\d+)_(.+)\.sql$`)
	noTransactionPattern = regexp.MustCompile(`(?m)^--pgt:no_transaction$`)
	downSplitComment     = "--pgt:down"
)

type Migration struct {
	Meta
	Up   Step
	Down Step
}

type Meta struct {
	Filename string
	Version  int
	Name     string
}

type StepConfig struct {
	DisableTransaction bool
	// Batched bool
}

type Step struct {
	Name   string
	SQL    string
	Pos    int
	Config StepConfig
}

type Loader struct {
	fsys fs.FS
}

func NewLoader(fsys fs.FS) *Loader {
	return &Loader{
		fsys: fsys,
	}
}

func (l *Loader) Load() ([]Migration, error) {
	migrations := []Migration{}

	dirEntries, err := fs.ReadDir(l.fsys, ".")
	if err != nil {
		return nil, err
	}

	for _, e := range dirEntries {
		if e.IsDir() {
			continue
		}

		matches := filenamePattern.FindStringSubmatch(e.Name())
		if matches == nil {
			continue
		}

		version64, err := strconv.ParseInt(matches[1], 10, 32)
		if err != nil {
			// The regexp already validated that the prefix is all digits so this *should* never fail
			return nil, err
		}
		version := int(version64)

		name := matches[2]

		// add one to account for version table migration 0 injected by
		// migrator
		orderVersion := len(migrations) + 1
		if version != orderVersion {
			return nil, fmt.Errorf("migration version %d does not match order position %d", version, orderVersion)
		}

		meta := Meta{
			Filename: e.Name(),
			Version:  version,
			Name:     name,
		}

		migration, err := l.loadMigration(meta)
		if err != nil {
			return nil, err
		}

		migrations = append(migrations, *migration)
	}

	return migrations, nil
}

func (l *Loader) loadMigration(meta Meta) (*Migration, error) {
	data, err := fs.ReadFile(l.fsys, meta.Filename)
	if err != nil {
		return nil, err
	}
	text := string(data)

	var upSql, downSql string
	var upCfg, downCfg StepConfig
	var downSQLPos int

	splitIdx := strings.Index(text, downSplitComment)
	if splitIdx >= 0 {
		upSql = strings.TrimRight(text[:splitIdx], "\r\n")

		i := splitIdx + len(downSplitComment)
		for ; i < len(text); i++ {
			if text[i] == '\n' {
				break
			}
		}
		if i >= len(text) {
			return nil, fmt.Errorf("migration has no SQL following \"%s\"", downSplitComment)
		}

		downSql = text[i:]
		downSQLPos = i
		if err := l.extractConfig(downSql, &downCfg); err != nil {
			return nil, fmt.Errorf("error in down SQL: %w", err)
		}
	} else {
		upSql = text
	}
	if err := l.extractConfig(upSql, &upCfg); err != nil {
		return nil, fmt.Errorf("error in up SQL: %w", err)
	}

	migration := &Migration{
		Meta: meta,
		Up: Step{
			Name:   "up",
			SQL:    upSql,
			Pos:    0,
			Config: upCfg,
		},
		Down: Step{
			Name:   "down",
			SQL:    downSql,
			Pos:    downSQLPos,
			Config: downCfg,
		},
	}

	return migration, nil
}

func (l *Loader) extractConfig(sql string, cfg *StepConfig) error {
	if noTransactionPattern.MatchString(sql) {
		cfg.DisableTransaction = true
	}
	return nil
}

// tmpl, err := template.New("up").
// 	Parse(string(text))
// if err != nil {
// 	return nil, err
// }

// var cfg MigrationConfig
// if cfgTmpl := tmpl.Lookup("config"); cfgTmpl != nil {
// 	b := bytes.Buffer{}
// 	if err := cfgTmpl.Execute(&b, nil); err != nil {
// 		return nil, err
// 	}
// 	if _, err := toml.NewDecoder(&b).Decode(&cfg); err != nil {
// 		return nil, err
// 	}
// }

// upSQL, err := l.executeTemplate(tmpl)
// if err != nil {
// 	return nil, err
// }

// downSQL := ""
// if downTmpl := tmpl.Lookup("down"); downTmpl != nil {
// 	s, err := l.executeTemplate(tmpl)
// 	if err != nil {
// 		return nil, err
// 	}
// 	downSQL = s
// }

// migration := &_Migration{
// 	MigrationMeta: meta,
// 	UpSQL: upSQL,
// 	DownSQL: downSQL,
// }

// func (l *Loader) executeTemplate(tmpl *template.Template) (string, error) {
// 	b := strings.Builder{}
// 	if err := tmpl.Execute(&b, l.data); err != nil {
// 		return "", err
// 	}
// 	return b.String(), nil
// }
