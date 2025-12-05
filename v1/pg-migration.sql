DO $$ 
BEGIN 
    IF NOT EXISTS (
        SELECT 1 
        FROM pg_type 
        WHERE typname = 'go_saga_task_enum'
    ) THEN 
        CREATE TYPE "go_saga_task_enum" AS ENUM ('wait', 'work', 'ready', 'error', 'rollback', 'error_rollback_none');
    END IF; 
END $$;

CREATE TABLE IF NOT EXISTS "go_saga_in_task" (
    "id" BIGINT PRIMARY KEY,
    "idempotency_key" VARCHAR NOT NULL,
    "data" JSON,
    "rollback_data" JSON,
    "status" "go_saga_task_enum" NOT NULL DEFAULT 'wait',
    "info" VARCHAR,
    "updated_at" TIMESTAMPTZ DEFAULT timezone('UTC', now()),
    UNIQUE ("idempotency_key")
);

CREATE INDEX "go_saga_in_task_idempotency_key_index" 
ON "go_saga_in_task" USING HASH ("idempotency_key");

CREATE INDEX "go_saga_in_task_status_index" 
ON "go_saga_in_task" USING HASH ("status");

CREATE TABLE IF NOT EXISTS "go_saga_out_task" (
    "id" BIGINT PRIMARY KEY,
    "idempotency_key" VARCHAR NOT NULL,
    "data" JSON,
    "rollback_data" JSON,
    "status" "go_saga_task_enum" NOT NULL DEFAULT 'wait',
    "info" VARCHAR,
    "updated_at" TIMESTAMPTZ DEFAULT timezone('UTC', now()),
    UNIQUE ("idempotency_key")
);

CREATE INDEX "go_saga_out_task_idempotency_key_index" 
ON "go_saga_out_task" USING HASH ("idempotency_key");

CREATE INDEX "go_saga_out_task_status_index" 
ON "go_saga_out_task" USING HASH ("status");

CREATE TABLE IF NOT EXISTS "go_saga_dlq_in_task" (
    "id" BIGINT PRIMARY KEY,
    "task_id" BIGINT,
    "time_for_next_try" INT NOT NULL DEFAULT 1,
    "time_mul" INT NOT NULL DEFAULT 2,
    "max_attempts" INT NOT NULL DEFAULT 5,
    "have_attempts" INT NOT NULL DEFAULT 0,
    "updated_at" TIMESTAMPTZ DEFAULT timezone('UTC', now()),
    UNIQUE ("task_id"),
    CONSTRAINT go_saga_fk_in_task FOREIGN KEY ("task_id") 
        REFERENCES "go_saga_in_task" ("id") ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS "go_saga_dlq_out_task" (
    "id" BIGINT PRIMARY KEY,
    "task_id" BIGINT,
    "time_for_next_try" INT NOT NULL DEFAULT 1,
    "time_mul" INT NOT NULL DEFAULT 2,
    "max_attempts" INT NOT NULL DEFAULT 5,
    "have_attempts" INT NOT NULL DEFAULT 0,
    "updated_at" TIMESTAMPTZ DEFAULT timezone('UTC', now()),
    UNIQUE ("task_id"),
    CONSTRAINT go_saga_fk_out_task FOREIGN KEY ("task_id") 
        REFERENCES "go_saga_out_task" ("id") ON DELETE CASCADE
);

CREATE OR REPLACE FUNCTION go_saga_update_task_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = timezone('UTC', now());
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION go_saga_increment_dlq_attempts()
RETURNS TRIGGER AS $$
BEGIN
    NEW.have_attempts := OLD.have_attempts + 1;
    NEW.time_for_next_try := OLD.time_for_next_try * OLD.time_mul;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER go_saga_update_dlq_out_task_updated_at
BEFORE UPDATE ON "go_saga_dlq_out_task"
FOR EACH ROW
EXECUTE FUNCTION go_saga_update_task_timestamp();

CREATE TRIGGER go_saga_dlq_out_task_attempt
BEFORE UPDATE ON "go_saga_dlq_out_task"
FOR EACH ROW
EXECUTE FUNCTION go_saga_increment_dlq_attempts();

CREATE TRIGGER go_saga_update_dlq_in_task_updated_at
BEFORE UPDATE ON "go_saga_dlq_in_task"
FOR EACH ROW
EXECUTE FUNCTION go_saga_update_task_timestamp();

CREATE TRIGGER go_saga_dlq_in_task_attempt
BEFORE UPDATE ON "go_saga_dlq_in_task"
FOR EACH ROW
EXECUTE FUNCTION go_saga_increment_dlq_attempts();

CREATE TRIGGER go_saga_update_out_task_updated_at
BEFORE UPDATE ON "go_saga_out_task"
FOR EACH ROW
EXECUTE FUNCTION go_saga_update_task_timestamp();

CREATE TRIGGER go_saga_update_in_task_updated_at
BEFORE UPDATE ON "go_saga_in_task"
FOR EACH ROW
EXECUTE FUNCTION go_saga_update_task_timestamp();
