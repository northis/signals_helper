-- sybmols.db
CREATE DATABASE "sybmols";

CREATE TABLE "AUDUSD"
(
	"DateTime" DATETIME NOT NULL UNIQUE,
	"Open" NUMERIC NOT NULL,
	"High" NUMERIC NOT NULL,
	"Low" NUMERIC NOT NULL,
	"Close" NUMERIC NOT NULL,
	PRIMARY KEY("DateTime")
);

CREATE UNIQUE INDEX "AUDUSD_DateIndex" ON "AUDUSD" (
	"DateTime"	DESC
);

CREATE TABLE "BTCUSD"
(
	"DateTime" DATETIME NOT NULL UNIQUE,
	"Open" NUMERIC NOT NULL,
	"High" NUMERIC NOT NULL,
	"Low" NUMERIC NOT NULL,
	"Close" NUMERIC NOT NULL,
	PRIMARY KEY("DateTime")
);

CREATE UNIQUE INDEX "BTCUSD_DateIndex" ON "BTCUSD" (
	"DateTime"	DESC
);

CREATE TABLE "EURUSD"
(
	"DateTime" DATETIME NOT NULL UNIQUE,
	"Open" NUMERIC NOT NULL,
	"High" NUMERIC NOT NULL,
	"Low" NUMERIC NOT NULL,
	"Close" NUMERIC NOT NULL,
	PRIMARY KEY("DateTime")
);

CREATE UNIQUE INDEX "EURUSD_DateIndex" ON "EURUSD" (
	"DateTime"	DESC
);

CREATE TABLE "GBPUSD"
(
	"DateTime" DATETIME NOT NULL UNIQUE,
	"Open" NUMERIC NOT NULL,
	"High" NUMERIC NOT NULL,
	"Low" NUMERIC NOT NULL,
	"Close" NUMERIC NOT NULL,
	PRIMARY KEY("DateTime")
);

CREATE UNIQUE INDEX "GBPUSD_DateIndex" ON "GBPUSD" (
	"DateTime"	DESC
);

CREATE TABLE "NZDUSD"
(
	"DateTime" DATETIME NOT NULL UNIQUE,
	"Open" NUMERIC NOT NULL,
	"High" NUMERIC NOT NULL,
	"Low" NUMERIC NOT NULL,
	"Close" NUMERIC NOT NULL,
	PRIMARY KEY("DateTime")
);

CREATE UNIQUE INDEX "NZDUSD_DateIndex" ON "NZDUSD" (
	"DateTime"	DESC
);

CREATE TABLE "USDCAD"
(
	"DateTime" DATETIME NOT NULL UNIQUE,
	"Open" NUMERIC NOT NULL,
	"High" NUMERIC NOT NULL,
	"Low" NUMERIC NOT NULL,
	"Close" NUMERIC NOT NULL,
	PRIMARY KEY("DateTime")
);

CREATE UNIQUE INDEX "USDCAD_DateIndex" ON "USDCAD" (
	"DateTime"	DESC
);

CREATE TABLE "USDCHF"
(
	"DateTime" DATETIME NOT NULL UNIQUE,
	"Open" NUMERIC NOT NULL,
	"High" NUMERIC NOT NULL,
	"Low" NUMERIC NOT NULL,
	"Close" NUMERIC NOT NULL,
	PRIMARY KEY("DateTime")
);

CREATE UNIQUE INDEX "USDCHF_DateIndex" ON "USDCHF" (
	"DateTime"	DESC
);

CREATE TABLE "USDJPY"
(
	"DateTime" DATETIME NOT NULL UNIQUE,
	"Open" NUMERIC NOT NULL,
	"High" NUMERIC NOT NULL,
	"Low" NUMERIC NOT NULL,
	"Close" NUMERIC NOT NULL,
	PRIMARY KEY("DateTime")
);

CREATE UNIQUE INDEX "USDJPY_DateIndex" ON "USDJPY" (
	"DateTime"	DESC
);

CREATE TABLE "USDRUB"
(
	"DateTime" DATETIME NOT NULL UNIQUE,
	"Open" NUMERIC NOT NULL,
	"High" NUMERIC NOT NULL,
	"Low" NUMERIC NOT NULL,
	"Close" NUMERIC NOT NULL,
	PRIMARY KEY("DateTime")
);

CREATE UNIQUE INDEX "USDRUB_DateIndex" ON "USDRUB" (
	"DateTime"	DESC
);

CREATE TABLE "XAGUSD"
(
	"DateTime" DATETIME NOT NULL UNIQUE,
	"Open" NUMERIC NOT NULL,
	"High" NUMERIC NOT NULL,
	"Low" NUMERIC NOT NULL,
	"Close" NUMERIC NOT NULL,
	PRIMARY KEY("DateTime")
);

CREATE UNIQUE INDEX "XAGUSD_DateIndex" ON "XAGUSD" (
	"DateTime"	DESC
);

CREATE TABLE "XAUUSD"
(
	"DateTime" DATETIME NOT NULL UNIQUE,
	"Open" NUMERIC NOT NULL,
	"High" NUMERIC NOT NULL,
	"Low" NUMERIC NOT NULL,
	"Close" NUMERIC NOT NULL,
	PRIMARY KEY("DateTime")
);

CREATE UNIQUE INDEX "XAUUSD_DateIndex" ON "XAUUSD" (
	"DateTime"	DESC
);

-- stats.db
CREATE DATABASE "stats";

CREATE TABLE "Channel"
(
	"Id" INTEGER NOT NULL UNIQUE,
	"Name" TEXT NOT NULL,
	"AccessLink" TEXT NOT NULL,
	"CreateDate" DATETIME NOT NULL,
	"UpdateDate" DATETIME,
	"HistoryLoaded" INTEGER,
	"HistoryUpdateDate" DATETIME,
	"HistoryAnalyzed" INTEGER,
	"HistoryAnalysisUpdateDate" DATETIME,
	PRIMARY KEY("Id")
)

CREATE UNIQUE INDEX "Channel_IdIndex" ON "Channel" (
	"Id"	DESC
);

CREATE UNIQUE INDEX "Channel_NameIndex" ON "Channel" (
	"Name" ASC
);

CREATE UNIQUE INDEX "Channel_AccessLinkIndex" ON "Channel" (
	"AccessLink" ASC
);

CREATE TABLE "ChannelMessageLink"
(
	"IdPrimary" INTEGER NOT NULL UNIQUE,
	"IdMessage" INTEGER,
	"IdChannel" INTEGER,
	PRIMARY KEY("IdPrimary")
);

CREATE UNIQUE INDEX "ChannelMessageLink_IdMessageIndex" ON "ChannelMessageLink" (
	"IdMessage"	ASC
);

CREATE UNIQUE INDEX "Channel_IdIndex" ON "ChannelMessageLink" (
	"IdMessage"	ASC,
	"IdChannel"	ASC
);


CREATE TABLE "Order"
(
	"IdChannel" INTEGER NOT NULL,
	"Symbol" TEXT NOT NULL,
	"IdOrder" INTEGER NOT NULL,
	"IsBuy" INTEGER NOT NULL DEFAULT 0,
	"Date" DATETIME NOT NULL,
	"PriceSignal" NUMERIC,
	"PriceActual" NUMERIC,
	"IsOpen" INTEGER NOT NULL DEFAULT 0,
	"StopLoss" NUMERIC,
	"TakeProfit" NUMERIC,
	"CloseDate" DATETIME,
	"ClosePrice" NUMERIC,
	"ManualExit" NUMERIC DEFAULT 0,
	"SlExit" NUMERIC DEFAULT 0,
	"TpExit" NUMERIC DEFAULT 0,
	"ErrorState" TEXT,
	FOREIGN KEY("IdChannel") REFERENCES "Channel"("Id")
);

CREATE TABLE "Signal"
(
	"IdChannel" INTEGER NOT NULL,
	"Symbol" TEXT NOT NULL,
	"IdMessage" INTEGER,
	"IsBuy" INTEGER NOT NULL DEFAULT 0,
	"Date" DATETIME NOT NULL,
	"PriceSignal" NUMERIC,
	"StopLoss" NUMERIC,
	"TakeProfit" NUMERIC,
	FOREIGN KEY("IdChannel") REFERENCES "Channel"("Id")
);
