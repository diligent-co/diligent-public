-- Create "aggregator_values" table
CREATE TABLE "aggregator_values" (
  "id" bigserial NOT NULL,
  "created_at" timestamptz NULL,
  "updated_at" timestamptz NULL,
  "aggregator_name" text NOT NULL,
  "as_of_time" timestamptz NOT NULL,
  "value" jsonb NOT NULL,
  PRIMARY KEY ("id")
);
-- Create index "idx_aggregator_values_aggregator_name" to table: "aggregator_values"
CREATE INDEX "idx_aggregator_values_aggregator_name" ON "aggregator_values" ("aggregator_name");
-- Create index "idx_aggregator_values_as_of_time" to table: "aggregator_values"
CREATE INDEX "idx_aggregator_values_as_of_time" ON "aggregator_values" ("as_of_time");
-- Create "feed_values" table
CREATE TABLE "feed_values" (
  "id" bigserial NOT NULL,
  "created_at" timestamptz NULL,
  "updated_at" timestamptz NULL,
  "feed_name" text NOT NULL,
  "timestamp" timestamptz NOT NULL,
  "feed_value" jsonb NOT NULL,
  PRIMARY KEY ("id")
);
-- Create index "idx_feed_values_feed_name" to table: "feed_values"
CREATE INDEX "idx_feed_values_feed_name" ON "feed_values" ("feed_name");
-- Create index "idx_feed_values_timestamp" to table: "feed_values"
CREATE INDEX "idx_feed_values_timestamp" ON "feed_values" ("timestamp");
