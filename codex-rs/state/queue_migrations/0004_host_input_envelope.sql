ALTER TABLE host_input_operations ADD COLUMN purpose TEXT NOT NULL DEFAULT 'legacy';
ALTER TABLE host_input_operations ADD COLUMN input_mode TEXT NOT NULL DEFAULT 'legacy';
ALTER TABLE host_input_operations ADD COLUMN target_json TEXT NOT NULL DEFAULT '{}';
