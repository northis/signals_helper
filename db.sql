CREATE TABLE "AUDUSD" (
	"DateTime"	DATETIME NOT NULL UNIQUE,
	"High"	NUMERIC NOT NULL,
	"Low"	NUMERIC NOT NULL,
	"Open"	NUMERIC NOT NULL,
	"Close"	NUMERIC NOT NULL,
	PRIMARY KEY("DateTime")
);

CREATE UNIQUE INDEX "AUDUSD_DateIndex" ON "AUDUSD" (
	"DateTime"	DESC
);

CREATE TABLE "BTCUSD" (
	"DateTime"	DATETIME NOT NULL UNIQUE,
	"High"	NUMERIC NOT NULL,
	"Low"	NUMERIC NOT NULL,
	"Open"	NUMERIC NOT NULL,
	"Close"	NUMERIC NOT NULL,
	PRIMARY KEY("DateTime")
);

CREATE UNIQUE INDEX "BTCUSD_DateIndex" ON "BTCUSD" (
	"DateTime"	DESC
);

CREATE TABLE "EURUSD" (
	"DateTime"	DATETIME NOT NULL UNIQUE,
	"High"	NUMERIC NOT NULL,
	"Low"	NUMERIC NOT NULL,
	"Open"	NUMERIC NOT NULL,
	"Close"	NUMERIC NOT NULL,
	PRIMARY KEY("DateTime")
);

CREATE UNIQUE INDEX "EURUSD_DateIndex" ON "EURUSD" (
	"DateTime"	DESC
);

CREATE TABLE "GBPUSD" (
	"DateTime"	DATETIME NOT NULL UNIQUE,
	"High"	NUMERIC NOT NULL,
	"Low"	NUMERIC NOT NULL,
	"Open"	NUMERIC NOT NULL,
	"Close"	NUMERIC NOT NULL,
	PRIMARY KEY("DateTime")
);

CREATE UNIQUE INDEX "GBPUSD_DateIndex" ON "GBPUSD" (
	"DateTime"	DESC
);

CREATE TABLE "NZDUSD" (
	"DateTime"	DATETIME NOT NULL UNIQUE,
	"High"	NUMERIC NOT NULL,
	"Low"	NUMERIC NOT NULL,
	"Open"	NUMERIC NOT NULL,
	"Close"	NUMERIC NOT NULL,
	PRIMARY KEY("DateTime")
);

CREATE UNIQUE INDEX "NZDUSD_DateIndex" ON "NZDUSD" (
	"DateTime"	DESC
);

CREATE TABLE "USDCAD" (
	"DateTime"	DATETIME NOT NULL UNIQUE,
	"High"	NUMERIC NOT NULL,
	"Low"	NUMERIC NOT NULL,
	"Open"	NUMERIC NOT NULL,
	"Close"	NUMERIC NOT NULL,
	PRIMARY KEY("DateTime")
);

CREATE UNIQUE INDEX "USDCAD_DateIndex" ON "USDCAD" (
	"DateTime"	DESC
);

CREATE TABLE "USDCHF" (
	"DateTime"	DATETIME NOT NULL UNIQUE,
	"High"	NUMERIC NOT NULL,
	"Low"	NUMERIC NOT NULL,
	"Open"	NUMERIC NOT NULL,
	"Close"	NUMERIC NOT NULL,
	PRIMARY KEY("DateTime")
);

CREATE UNIQUE INDEX "USDCHF_DateIndex" ON "USDCHF" (
	"DateTime"	DESC
);

CREATE TABLE "USDJPY" (
	"DateTime"	DATETIME NOT NULL UNIQUE,
	"High"	NUMERIC NOT NULL,
	"Low"	NUMERIC NOT NULL,
	"Open"	NUMERIC NOT NULL,
	"Close"	NUMERIC NOT NULL,
	PRIMARY KEY("DateTime")
);

CREATE UNIQUE INDEX "USDJPY_DateIndex" ON "USDJPY" (
	"DateTime"	DESC
);

CREATE TABLE "USDRUB" (
	"DateTime"	DATETIME NOT NULL UNIQUE,
	"High"	NUMERIC NOT NULL,
	"Low"	NUMERIC NOT NULL,
	"Open"	NUMERIC NOT NULL,
	"Close"	NUMERIC NOT NULL,
	PRIMARY KEY("DateTime")
);

CREATE UNIQUE INDEX "USDRUB_DateIndex" ON "USDRUB" (
	"DateTime"	DESC
);

CREATE TABLE "XAGUSD" (
	"DateTime"	DATETIME NOT NULL UNIQUE,
	"High"	NUMERIC NOT NULL,
	"Low"	NUMERIC NOT NULL,
	"Open"	NUMERIC NOT NULL,
	"Close"	NUMERIC NOT NULL,
	PRIMARY KEY("DateTime")
);

CREATE UNIQUE INDEX "XAGUSD_DateIndex" ON "XAGUSD" (
	"DateTime"	DESC
);

CREATE TABLE "XAUUSD" (
	"DateTime"	DATETIME NOT NULL UNIQUE,
	"High"	NUMERIC NOT NULL,
	"Low"	NUMERIC NOT NULL,
	"Open"	NUMERIC NOT NULL,
	"Close"	NUMERIC NOT NULL,
	PRIMARY KEY("DateTime")
);

CREATE UNIQUE INDEX "XAUUSD_DateIndex" ON "XAUUSD" (
	"DateTime"	DESC
);