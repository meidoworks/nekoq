CREATE DATABASE "nekoq"
WITH
  OWNER = "admin"
  ENCODING = 'UTF8'
;



CREATE TABLE "public"."simple_msg" (
  "msg_id" bigserial NOT NULL,
  "msg_status" int2 NOT NULL,
  "msg_deleted" int2 NOT NULL,
  "topic" varchar(255) NOT NULL,
  "queue_name" varchar(255) NOT NULL,
  "header" text NOT NULL,
  "payload" bytea NOT NULL,
  "time_created" int8 NOT NULL,
  "time_delivered" int8 NULL,
  "schedule_time" int8 NOT NULL,
  PRIMARY KEY ("msg_id")
)
;

CREATE INDEX "idx_to_be_deliver" ON "public"."simple_msg" (
  "queue_name",
  "schedule_time" ASC
) WHERE msg_deleted = 0;

CREATE INDEX "idx_undelivered" ON "public"."simple_msg" (
  "queue_name"
) WHERE msg_status = 3 and msg_deleted = 1;

COMMENT ON COLUMN "public"."simple_msg"."msg_status" IS '0 - ready
1 - in flight
2 - delivered
3 - undelivered';

COMMENT ON COLUMN "public"."simple_msg"."msg_deleted" IS '0 - active
1 - deleted';

COMMENT ON COLUMN "public"."simple_msg"."schedule_time" IS 'scheduled deliver time';



CREATE TABLE "public"."simple_topic_queue_mapping" (

)
;

ALTER TABLE "public"."simple_topic_queue_mapping"
  ADD COLUMN "mapping_id" bigserial NOT NULL,
  ADD COLUMN "topic" varchar(255),
  ADD COLUMN "queue_name" varchar(255),
  ADD PRIMARY KEY ("mapping_id");

CREATE INDEX "idx_topic" ON "public"."simple_topic_queue_mapping" (
  "topic"
);

ALTER TABLE "public"."simple_topic_queue_mapping"
  ADD CONSTRAINT "uk_queuename" UNIQUE ("queue_name");
ALTER TABLE "public"."simple_topic_queue_mapping"
  ADD CONSTRAINT "uk_topic_queue" UNIQUE ("topic", "queue_name");
