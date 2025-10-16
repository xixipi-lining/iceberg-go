package table

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"github.com/apache/iceberg-go"
)

func (t *Transaction) AddFilesChange(ctx context.Context, files []string, snapshotProps iceberg.Properties, ignoreDuplicates bool) ([]Update, []Requirement, error) {
	set := make(map[string]string)
	for _, f := range files {
		set[f] = f
	}

	if len(set) != len(files) {
		return nil, nil, errors.New("file paths must be unique for AddFiles")
	}

	if !ignoreDuplicates {
		if s := t.meta.currentSnapshot(); s != nil {
			referenced := make([]string, 0)
			fs, err := t.tbl.fsF(ctx)
			if err != nil {
				return nil, nil, err
			}
			for df, err := range s.dataFiles(fs, nil) {
				if err != nil {
					return nil, nil, err
				}

				if _, ok := set[df.FilePath()]; ok {
					referenced = append(referenced, df.FilePath())
				}
			}
			if len(referenced) > 0 {
				return nil, nil, fmt.Errorf("cannot add files that are already referenced by table, files: %s", referenced)
			}
		}
	}

	if t.meta.NameMapping() == nil {
		nameMapping := t.meta.CurrentSchema().NameMapping()
		mappingJson, err := json.Marshal(nameMapping)
		if err != nil {
			return nil, nil, err
		}
		err = t.SetProperties(iceberg.Properties{DefaultNameMappingKey: string(mappingJson)})
		if err != nil {
			return nil, nil, err
		}
	}

	fs, err := t.tbl.fsF(ctx)
	if err != nil {
		return nil, nil, err
	}

	updater := t.updateSnapshot(fs, snapshotProps).fastAppend()

	dataFiles := filesToDataFiles(ctx, fs, t.meta, slices.Values(files))
	for df, err := range dataFiles {
		if err != nil {
			return nil, nil, err
		}
		updater.appendDataFile(df)
	}

	return updater.commit()
}