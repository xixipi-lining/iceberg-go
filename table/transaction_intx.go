package table

import (
	"context"
	"errors"
)

func (t *Transaction) CommitInTx(ctx context.Context) (*Table, error) {
	t.mx.Lock()
	defer t.mx.Unlock()

	if t.committed {
		return nil, errors.New("transaction has already been committed")
	}

	t.committed = true

	if len(t.meta.updates) > 0 {
		t.reqs = append(t.reqs, AssertTableUUID(t.meta.uuid))
		tbl, err := t.tbl.doCommitInTx(ctx, t.meta.updates, t.reqs)
		if err != nil {
			return tbl, err
		}

		for _, u := range t.meta.updates {
			if perr := u.PostCommit(ctx, t.tbl, tbl); perr != nil {
				err = errors.Join(err, perr)
			}
		}

		return tbl, err
	}

	return t.tbl, nil
}
