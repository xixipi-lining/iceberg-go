package table

import "context"

type MultiTableTransactionCatalog interface {
	CommitTableInTx(ctx context.Context, identifier Identifier, reqs []Requirement, updates []Update) (Metadata, string, error)
}

func (t Table) doCommitInTx(ctx context.Context, updates []Update, reqs []Requirement) (*Table, error) {
	cat := t.cat.(MultiTableTransactionCatalog)
	newMeta, newLoc, err := cat.CommitTableInTx(ctx, t.identifier, reqs, updates)
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
