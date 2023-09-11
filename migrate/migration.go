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
}

type Step struct {
	Name       string
	Config     StepConfig
	SQL        string
	LineNumber int
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
			return nil, fmt.Errorf("error parsing version number in filename %s: %w", e.Name(), err)
		}
		version := int(version64)

		name := matches[2]

		// Add one to account for version table migration injected by migrator.
		orderVersion := len(migrations) + 1
		if version != orderVersion {
			return nil, fmt.Errorf("error loading migration %s: version %d does not match order position %d", e.Name(), version, orderVersion)
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
	var downSqlLineNumber int

	splitIdx := strings.Index(text, downSplitComment)
	if splitIdx >= 0 {
		upSql = strings.TrimRight(text[:splitIdx], "\r\n")

		// Advance until the next line to skip the split comment (the comment
		// may have more characters than the regex matches).
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

		// Count number of line breaks leading up to the down SQL.
		downSqlLineNumber = 1
		for _, c := range text[:i] {
			if c == '\n' {
				downSqlLineNumber += 1
			}
		}
	} else {
		upSql = text
	}

	if err := l.extractConfig(upSql, &upCfg); err != nil {
		return nil, fmt.Errorf("error in up SQL: %w", err)
	}

	if downSql != "" {
		if err := l.extractConfig(downSql, &downCfg); err != nil {
			return nil, fmt.Errorf("error in down SQL: %w", err)
		}
	}

	migration := &Migration{
		Meta: meta,
		Up: Step{
			Name:       "up",
			Config:     upCfg,
			SQL:        upSql,
			LineNumber: 1,
		},
		Down: Step{
			Name:       "down",
			Config:     downCfg,
			SQL:        downSql,
			LineNumber: downSqlLineNumber,
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

func (s Step) PositionToLineCol(pos int) (int, int) {
	if s.LineNumber == 0 {
		// LineNumber of 0 means we can't map back to a source.
		return 0, 0
	}
	cpos := 1
	line := s.LineNumber
	col := 1
	for _, c := range s.SQL {
		if cpos >= pos {
			break
		}
		if c == '\n' {
			line += 1
			col = 1
		} else {
			col += 1
		}
		cpos += 1
	}
	return line, col
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
