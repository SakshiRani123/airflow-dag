-- ====================================================================
-- Project: DHAP-34 (3K Conversations Dataset for ChatBot)
-- Target Table: conversations
-- Author: Sakshi
-- Status: done
-- ====================================================================

CREATE TABLE IF NOT EXISTS conversations (
    conversation_id INTEGER PRIMARY KEY,
    question TEXT NOT NULL,
    answer TEXT NOT NULL
);
