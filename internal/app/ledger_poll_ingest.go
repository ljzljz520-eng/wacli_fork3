package app

import (
	"context"
	"strconv"
	"time"

	"github.com/openclaw/wacli/internal/ledger"
	"github.com/openclaw/wacli/internal/store"
)

// UpsertPollWithLedger appends the poll creation before the direct write.
func (a *App) UpsertPollWithLedger(ctx context.Context, source string, p store.Poll) error {
	if ledgerEnabled() {
		createdAt := p.CreatedAt
		if createdAt.IsZero() {
			createdAt = nowUTC()
		}
		options := p.Options
		if options == nil {
			options = []string{}
		}
		c := ledger.CanonicalPoll{
			ChatJID:         p.ChatJID,
			MsgID:           p.MsgID,
			SenderJID:       p.SenderJID,
			Question:        p.Question,
			Options:         options,
			SelectableCount: p.SelectableCount,
			CreatedTS:       createdAt.Unix(),
		}
		dk := ledger.DedupKey("poll", c.ChatJID, c.MsgID)
		if err := a.appendCanonicalExtra(ctx, source, ledger.EventPoll,
			c.ChatJID, c, c.CreatedTS, dk, nil); err != nil {
			return err
		}
	}
	return a.db.UpsertPoll(p)
}

// AppendPollOptionWithLedger records one single-option poll update. The
// caller performs the read/merge/direct-write itself.
func (a *App) AppendPollOptionWithLedger(ctx context.Context, source,
	chatJID, pollMsgID, option string,
) error {
	if !ledgerEnabled() {
		return nil
	}
	c := ledger.CanonicalPollOptionAdd{
		ChatJID:   chatJID,
		PollMsgID: pollMsgID,
		Option:    option,
	}
	dk := ledger.DedupKey("poll-option", c.ChatJID, c.PollMsgID, c.Option)
	refs := []ledger.CausalRef{ledger.WAKeyRef(c.ChatJID, c.PollMsgID, "", false)}
	return a.appendCanonicalExtra(ctx, source, ledger.EventPollOption,
		c.ChatJID, c, 0, dk, refs)
}

// UpsertPollVoteWithLedger appends the vote before the direct write.
func (a *App) UpsertPollVoteWithLedger(ctx context.Context, source string, v store.PollVote) error {
	if ledgerEnabled() {
		if err := a.appendPollVoteEvent(ctx, source, v); err != nil {
			return err
		}
	}
	return a.db.UpsertPollVote(v)
}

// DeletePollVoteWithLedger appends the vote retraction marker before the
// direct delete.
func (a *App) DeletePollVoteWithLedger(ctx context.Context, source,
	chatJID, pollMsgID, voterJID string, votedAt time.Time,
) error {
	if ledgerEnabled() {
		votedTS := votedAt
		if votedTS.IsZero() {
			votedTS = nowUTC()
		}
		c := ledger.CanonicalPollVote{
			ChatJID:   chatJID,
			PollMsgID: pollMsgID,
			VoterJID:  voterJID,
			Selected:  []string{},
			VotedTSMS: votedTS.UnixMilli(),
			Deleted:   true,
		}
		dk := ledger.DedupKey("poll-vote-delete", c.ChatJID, c.PollMsgID,
			c.VoterJID, strconv.FormatInt(c.VotedTSMS, 10))
		refs := []ledger.CausalRef{ledger.WAKeyRef(c.ChatJID, c.PollMsgID, "", false)}
		if err := a.appendCanonicalExtra(ctx, source, ledger.EventPollVote,
			c.ChatJID, c, votedTS.Unix(), dk, refs); err != nil {
			return err
		}
	}
	return a.db.DeletePollVote(chatJID, pollMsgID, voterJID, votedAt)
}

func (a *App) appendPollVoteEvent(ctx context.Context, source string, v store.PollVote) error {
	selected := v.Selected
	if selected == nil {
		selected = []string{}
	}
	votedTS := v.VotedAt
	if votedTS.IsZero() {
		votedTS = nowUTC()
	}
	c := ledger.CanonicalPollVote{
		ChatJID:       v.ChatJID,
		PollMsgID:     v.PollMsgID,
		VoterJID:      v.VoterJID,
		VoteMsgID:     v.VoteMsgID,
		Selected:      selected,
		UnknownHashes: v.UnknownHashes,
		VotedTSMS:     votedTS.UnixMilli(),
	}
	dk := ledger.DedupKey("poll-vote", c.ChatJID, c.PollMsgID,
		c.VoterJID, strconv.FormatInt(c.VotedTSMS, 10))
	refs := []ledger.CausalRef{ledger.WAKeyRef(c.ChatJID, c.PollMsgID, "", false)}
	return a.appendCanonicalExtra(ctx, source, ledger.EventPollVote,
		c.ChatJID, c, votedTS.Unix(), dk, refs)
}
