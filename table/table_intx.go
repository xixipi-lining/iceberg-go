package table

import "context"

func (t Table) doCommitInTx(ctx context.Context, tx MultiTableTransaction, updates []Update, reqs []Requirement) (*Table, error) {
	newMeta, newLoc, err := tx.CommitTableInTx(ctx, t.identifier, reqs, updates)
	if err != nil {
		return nil, err
	}
	fs, err := t.fsF(ctx)
	if err != nil {
		return nil, err
	}
	deleteOldMetadata(fs, t.metadata, newMeta)

	return New(t.identifier, newMeta, newLoc, t.fsF, t.cat), nil
}
