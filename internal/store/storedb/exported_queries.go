package storedb

// Non-generated exports of generated SQL, so the store layer can build
// retargeted (shadow-table) variants of the same merge statements.

// UpsertMessageSQL is the generated message upsert/merge statement.
const UpsertMessageSQL = upsertMessage

// UpsertMessageLocationSQL is the generated message-location upsert.
const UpsertMessageLocationSQL = upsertMessageLocation

// MarkMessageDeletedForMeSQL is the generated delete-for-me mark statement.
const MarkMessageDeletedForMeSQL = markMessageDeletedForMe

// UpsertChatSQL is the generated chat merge statement.
const UpsertChatSQL = upsertChat

// UpsertContactSQL is the generated contact merge statement.
const UpsertContactSQL = upsertContact

// UpsertGroupWithHierarchySQL is the generated group hierarchy upsert.
const UpsertGroupWithHierarchySQL = upsertGroupWithHierarchy

// MarkGroupLeftSQL is the generated group leave mark statement.
const MarkGroupLeftSQL = markGroupLeft

// InsertGroupParticipantSQL is the generated roster insert statement.
const InsertGroupParticipantSQL = insertGroupParticipant

// DeleteGroupParticipantsSQL deletes every roster row of one group.
const DeleteGroupParticipantsSQL = deleteGroupParticipants

// SetStarredUpsertSQL is the generated star upsert statement.
const SetStarredUpsertSQL = setStarredUpsert

// SetStarredDeleteSQL is the generated star delete statement.
const SetStarredDeleteSQL = setStarredDelete
