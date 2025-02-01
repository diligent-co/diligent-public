-- Create "coinbot_trades" table
CREATE TABLE "coinbot_trades" (
  "id" bigserial NOT NULL,
  "created_at" timestamptz NULL,
  "updated_at" timestamptz NULL,
  "symbol" text NOT NULL,
  "order_side" text NOT NULL,
  "price" numeric NOT NULL,
  "quantity" numeric NOT NULL,
  "order_type" text NOT NULL,
  "timestamp" timestamptz NOT NULL,
  PRIMARY KEY ("id")
);
-- Create index "idx_coinbot_trades_order_type" to table: "coinbot_trades"
CREATE INDEX "idx_coinbot_trades_order_type" ON "coinbot_trades" ("order_type");
-- Create index "idx_coinbot_trades_symbol" to table: "coinbot_trades"
CREATE INDEX "idx_coinbot_trades_symbol" ON "coinbot_trades" ("symbol");
-- Create index "idx_coinbot_trades_timestamp" to table: "coinbot_trades"
CREATE INDEX "idx_coinbot_trades_timestamp" ON "coinbot_trades" ("timestamp");
-- Create "kraken_transaction_histories" table
CREATE TABLE "kraken_transaction_histories" (
  "id" bigserial NOT NULL,
  "created_at" timestamptz NULL,
  "updated_at" timestamptz NULL,
  "timestamp" timestamptz NOT NULL,
  "symbol1" text NOT NULL,
  "symbol2" text NOT NULL,
  "symbol1_amount" numeric NOT NULL,
  "symbol2_amount" numeric NOT NULL,
  PRIMARY KEY ("id")
);
-- Create index "idx_kraken_transaction_histories_symbol1" to table: "kraken_transaction_histories"
CREATE INDEX "idx_kraken_transaction_histories_symbol1" ON "kraken_transaction_histories" ("symbol1");
-- Create index "idx_kraken_transaction_histories_symbol2" to table: "kraken_transaction_histories"
CREATE INDEX "idx_kraken_transaction_histories_symbol2" ON "kraken_transaction_histories" ("symbol2");
-- Create index "idx_kraken_transaction_histories_timestamp" to table: "kraken_transaction_histories"
CREATE INDEX "idx_kraken_transaction_histories_timestamp" ON "kraken_transaction_histories" ("timestamp");
