package main

import (
	"context"
	"fmt"
	"os"

	"github.com/openclaw/wacli/internal/out"
	"github.com/openclaw/wacli/internal/store"
	"github.com/spf13/cobra"
)

func newLedgerCmd(flags *rootFlags) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "ledger",
		Short: "Inspect and manage the append-only event ledger and projected views",
	}
	cmd.AddCommand(newLedgerStatusCmd(flags))
	cmd.AddCommand(newLedgerRebuildCmd(flags))
	cmd.AddCommand(newLedgerVerifyCmd(flags))
	cmd.AddCommand(newLedgerDiffCmd(flags))
	cmd.AddCommand(newLedgerPromoteCmd(flags))
	cmd.AddCommand(newLedgerRollbackCmd(flags))
	return cmd
}

func newLedgerStatusCmd(flags *rootFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show head seq, per-view checkpoints, lag, raw/scrub counts",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := withTimeout(context.Background(), flags)
			defer cancel()
			a, lk, err := newApp(ctx, flags, false, true)
			if err != nil {
				return err
			}
			defer closeApp(a, lk)

			rep, err := a.LedgerStatus(ctx)
			if err != nil {
				return err
			}
			if flags.asJSON {
				return out.WriteJSON(os.Stdout, rep)
			}
			writeLedgerStatus(os.Stdout, rep)
			return nil
		},
	}
}

func newLedgerRebuildCmd(flags *rootFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "rebuild",
		Short: "Rebuild shadow views by replaying the full ledger",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := flags.requireWritable(); err != nil {
				return err
			}
			ctx, cancel := withTimeout(context.Background(), flags)
			defer cancel()
			a, lk, err := newApp(ctx, flags, true, true)
			if err != nil {
				return err
			}
			defer closeApp(a, lk)

			rep, err := a.RebuildShadow(ctx)
			if err != nil {
				return err
			}
			if flags.asJSON {
				return out.WriteJSON(os.Stdout, rep)
			}
			fmt.Fprintf(os.Stdout, "Rebuilt %d views through head seq %d.\n", len(rep.Views), rep.TargetSeq)
			return nil
		},
	}
}

func newLedgerVerifyCmd(flags *rootFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "verify",
		Short: "Run cross-view invariant checks against active views",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := withTimeout(context.Background(), flags)
			defer cancel()
			a, lk, err := newApp(ctx, flags, false, true)
			if err != nil {
				return err
			}
			defer closeApp(a, lk)

			rep, err := a.VerifyLedger(ctx)
			if err != nil {
				return err
			}
			if flags.asJSON {
				if err := out.WriteJSON(os.Stdout, rep); err != nil {
					return err
				}
			} else {
				writeVerifyReport(os.Stdout, rep)
			}
			if rep.HasViolations() {
				return fmt.Errorf("ledger verify found %d violations", len(rep.Violations))
			}
			return nil
		},
	}
}

func newLedgerDiffCmd(flags *rootFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "diff",
		Short: "Compare active views with the rebuilt shadow views",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := withTimeout(context.Background(), flags)
			defer cancel()
			a, lk, err := newApp(ctx, flags, false, true)
			if err != nil {
				return err
			}
			defer closeApp(a, lk)

			rep, err := a.DiffShadowLedger(ctx)
			if err != nil {
				return err
			}
			if flags.asJSON {
				if err := out.WriteJSON(os.Stdout, rep); err != nil {
					return err
				}
			} else {
				writeDiffReport(os.Stdout, rep)
			}
			if rep.HasDiffs() {
				return fmt.Errorf("shadow diff found %d differing rows", len(rep.Diffs))
			}
			return nil
		},
	}
}

func newLedgerPromoteCmd(flags *rootFlags) *cobra.Command {
	var force bool
	cmd := &cobra.Command{
		Use:   "promote",
		Short: "Atomically switch the rebuilt shadow views to active",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := flags.requireWritable(); err != nil {
				return err
			}
			ctx, cancel := withTimeout(context.Background(), flags)
			defer cancel()
			a, lk, err := newApp(ctx, flags, true, true)
			if err != nil {
				return err
			}
			defer closeApp(a, lk)

			rep, err := a.PromoteShadow(ctx, force)
			if err != nil {
				return err
			}
			if flags.asJSON {
				return out.WriteJSON(os.Stdout, rep)
			}
			fmt.Fprintf(os.Stdout, "Promoted %d views at head seq %d.\n", rep.Tables, rep.HeadSeq)
			return nil
		},
	}
	cmd.Flags().BoolVar(&force, "force", false, "bypass the diff policy (invariant checks always apply)")
	return cmd
}

func newLedgerRollbackCmd(flags *rootFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "rollback",
		Short: "Atomically restore the previous active views",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := flags.requireWritable(); err != nil {
				return err
			}
			ctx, cancel := withTimeout(context.Background(), flags)
			defer cancel()
			a, lk, err := newApp(ctx, flags, true, true)
			if err != nil {
				return err
			}
			defer closeApp(a, lk)

			rep, err := a.RollbackLedger(ctx)
			if err != nil {
				return err
			}
			if flags.asJSON {
				return out.WriteJSON(os.Stdout, rep)
			}
			fmt.Fprintf(os.Stdout, "Rolled back %d views at head seq %d.\n", rep.Tables, rep.HeadSeq)
			return nil
		},
	}
}

func writeLedgerStatus(w *os.File, rep *store.LedgerStatusReport) {
	tw := newTableWriter(w)
	fmt.Fprintf(tw, "HEAD_SEQ\t%d\n", rep.HeadSeq)
	fmt.Fprintf(tw, "RAW_EVENTS\t%d\n", rep.RawCount)
	fmt.Fprintf(tw, "SCRUB_EVENTS\t%d\n", rep.ScrubCount)
	fmt.Fprintf(tw, "FTS5\t%v\n", rep.FTSEnabled)
	fmt.Fprintf(tw, "SHADOW_SET\t%v\n", rep.ShadowSet)
	fmt.Fprintf(tw, "BACKUP_SET\t%v\n", rep.BackupSet)
	for _, v := range rep.Views {
		fmt.Fprintf(tw, "VIEW\t%s\t%s\tseq=%d\tlag=%d\n", v.View, v.Mode, v.Seq, v.Lag)
	}
	_ = tw.Flush()
}

func writeVerifyReport(w *os.File, rep *store.VerifyReport) {
	tw := newTableWriter(w)
	for _, v := range rep.Violations {
		fmt.Fprintf(tw, "%s\tseq=%d\t%s\t%s\n", v.Check, v.Seq, sanitize(v.Key), sanitize(v.Detail))
	}
	_ = tw.Flush()
	if !rep.HasViolations() {
		fmt.Fprintln(w, "OK: no ledger violations.")
	}
}

func writeDiffReport(w *os.File, rep *store.DiffReport) {
	tw := newTableWriter(w)
	for table, c := range rep.Counts {
		fmt.Fprintf(tw, "COUNT\t%s\tactive=%d\tshadow=%d\n", table, c.Active, c.Shadow)
	}
	for _, d := range rep.Diffs {
		fmt.Fprintf(tw, "%s\t%s\t%s\tseq=%d\tevent=%s\tfields=%s\n",
			d.Kind, d.Table, sanitize(d.Key), d.Seq, d.EventID, joinComma(d.Fields))
	}
	_ = tw.Flush()
	if !rep.HasDiffs() {
		fmt.Fprintln(w, "OK: shadow matches active.")
	}
}

func joinComma(items []string) string {
	out := ""
	for i, item := range items {
		if i > 0 {
			out += ","
		}
		out += item
	}
	return out
}
