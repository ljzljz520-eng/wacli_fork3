package projector

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/openclaw/wacli/internal/wa"
)

// BaseDisplayText mirrors app.baseDisplayText: call log > media label > text.
func BaseDisplayText(pm wa.ParsedMessage) string {
	if pm.Call != nil {
		return CallDisplayText(*pm.Call)
	}
	if pm.Media != nil {
		return "Sent " + MediaLabel(pm.Media.Type)
	}
	if text := strings.TrimSpace(pm.Text); text != "" {
		return text
	}
	return ""
}

// CallDisplayText mirrors app.callDisplayText.
func CallDisplayText(call wa.ParsedCallEvent) string {
	parts := []string{"WhatsApp"}
	if call.Media != "" {
		parts = append(parts, call.Media)
	}
	parts = append(parts, "call")
	if call.Outcome != "" {
		parts = append(parts, call.Outcome)
	} else if call.EventType != "" && call.EventType != "call_log" {
		parts = append(parts, call.EventType)
	}
	if call.DurationSecs > 0 {
		parts = append(parts, fmt.Sprintf("(%s)", formatCallDuration(call.DurationSecs)))
	}
	return strings.Join(parts, " ")
}

func formatCallDuration(seconds int64) string {
	if seconds <= 0 {
		return ""
	}
	minutes := seconds / 60
	secs := seconds % 60
	if minutes <= 0 {
		return fmt.Sprintf("%ds", secs)
	}
	if secs == 0 {
		return fmt.Sprintf("%dm", minutes)
	}
	return fmt.Sprintf("%dm%02ds", minutes, secs)
}

// MediaLabel mirrors app.mediaLabel.
func MediaLabel(mediaType string) string {
	switch strings.ToLower(strings.TrimSpace(mediaType)) {
	case "gif":
		return "gif"
	case "image":
		return "image"
	case "video":
		return "video"
	case "audio":
		return "audio"
	case "sticker":
		return "sticker"
	case "document":
		return "document"
	case "location":
		return "location"
	case "live_location":
		return "live location"
	case "contact":
		return "contact"
	case "contacts":
		return "contacts"
	case "":
		return "message"
	default:
		return strings.ToLower(strings.TrimSpace(mediaType))
	}
}

// lookupMessageDisplay mirrors app.lookupMessageDisplayText against an
// arbitrary target table inside tx.
func lookupMessageDisplay(ctx context.Context, tx *sql.Tx, target, chatJID, msgID string) string {
	if strings.TrimSpace(chatJID) == "" || strings.TrimSpace(msgID) == "" {
		return ""
	}
	var display, text, mediaType string
	err := tx.QueryRowContext(ctx,
		`SELECT COALESCE(display_text,''), COALESCE(text,''), COALESCE(media_type,'')
		 FROM `+target+` WHERE chat_jid = ? AND msg_id = ?`,
		strings.TrimSpace(chatJID), strings.TrimSpace(msgID),
	).Scan(&display, &text, &mediaType)
	if err != nil {
		return ""
	}
	if s := strings.TrimSpace(display); s != "" {
		return s
	}
	if s := strings.TrimSpace(text); s != "" {
		return s
	}
	if strings.TrimSpace(mediaType) != "" {
		return "Sent " + MediaLabel(mediaType)
	}
	return ""
}

// buildDisplayText mirrors App.buildDisplayText against an arbitrary target.
func buildDisplayText(ctx context.Context, tx *sql.Tx, target string, pm wa.ParsedMessage) string {
	base := BaseDisplayText(pm)

	if pm.ReactionToID != "" || strings.TrimSpace(pm.ReactionEmoji) != "" {
		targetID := strings.TrimSpace(pm.ReactionToID)
		display := ""
		if targetID != "" {
			display = lookupMessageDisplay(ctx, tx, target, pm.Chat.String(), targetID)
		}
		if display == "" {
			display = "message"
		}
		if emoji := strings.TrimSpace(pm.ReactionEmoji); emoji != "" {
			return fmt.Sprintf("Reacted %s to %s", emoji, display)
		}
		return fmt.Sprintf("Reacted to %s", display)
	}

	if pm.ReplyToID != "" {
		quoted := strings.TrimSpace(pm.ReplyToDisplay)
		if quoted == "" {
			quoted = lookupMessageDisplay(ctx, tx, target, pm.Chat.String(), pm.ReplyToID)
		}
		if quoted == "" {
			quoted = "message"
		}
		if base == "" {
			base = "(message)"
		}
		return fmt.Sprintf("> %s\n%s", quoted, base)
	}

	if base == "" {
		base = "(message)"
	}
	return base
}
