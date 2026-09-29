"""Tests du générateur : intégrité référentielle, schémas, devises, KPIs réalistes."""

from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd
import pytest

from waba_gen import config as cfg
from waba_gen.engine import load_referentials, publish_referentials, run_one_time
from waba_gen.storage import LocalSink
from waba_gen.transactions import RefIndex
from waba_gen.utils import is_valid_iban

START, END = date(2026, 7, 1), date(2026, 9, 30)

EXPECTED_COLUMNS = {
    "bank_transactions": ["transaction_id", "timestamp", "account_id", "beneficiary_account", "branch_id",
                          "country_code", "transaction_type", "amount", "currency", "channel",
                          "transaction_status", "fee_amount", "entity_type"],
    "insurance_operations": ["operation_id", "timestamp", "customer_id", "account_id", "country_code",
                             "operation_type", "product_line", "amount", "currency", "claim_status",
                             "processing_days", "entity_type"],
    "mobile_money_payments": ["payment_id", "timestamp", "sender_id", "receiver_id", "sender_country",
                              "receiver_country", "amount", "currency", "payment_type", "operator",
                              "status", "fee_amount", "entity_type", "country_code"],
    "loan_repayments": ["repayment_id", "timestamp", "loan_account_id", "customer_id", "country_code",
                        "amount_due", "amount_paid", "currency", "due_date", "payment_date", "days_overdue",
                        "loan_type", "repayment_status", "entity_type"],
}


@pytest.fixture(scope="module")
def world(tmp_path_factory):
    root = tmp_path_factory.mktemp("raw")
    sink = LocalSink(root)
    publish_referentials(sink, seed=7, scale=0.02)
    refs = load_referentials(sink)
    idx = RefIndex.build(refs)
    rows = {ds: n * 2 for ds, n in cfg.DEFAULT_ROWS.items()}
    run_one_time(sink, idx, list(cfg.DATASETS), rows, cfg.COUNTRIES, cfg.ENTITY_TYPES, START, END, seed=7)
    data = {}
    for ds, meta in cfg.DATASETS.items():
        files = sorted((root / meta["folder"]).rglob("*.csv"))
        data[ds] = pd.concat((pd.read_csv(f) for f in files), ignore_index=True)
    return refs, data


def test_referentials_keys_unique(world):
    refs, _ = world
    assert refs.customers["customer_id"].is_unique
    assert refs.accounts["account_id"].is_unique
    assert refs.branches["branch_id"].is_unique
    assert len(refs.branches) == 200 and len(refs.products) == 50


def test_accounts_reference_customers_same_country(world):
    refs, _ = world
    merged = refs.accounts.merge(refs.customers[["customer_id", "country_code"]], on="customer_id", suffixes=("", "_c"))
    assert len(merged) == len(refs.accounts), "compte orphelin"
    assert (merged["country_code"] == merged["country_code_c"]).all()


def test_iban_valid(world):
    refs, _ = world
    ibans = refs.accounts["iban"].dropna().sample(500, random_state=0)
    assert all(is_valid_iban(i) for i in ibans)
    assert refs.accounts.loc[refs.accounts["country_code"].eq("GH"), "iban"].isna().all()


@pytest.mark.parametrize("dataset", list(cfg.DATASETS))
def test_schema_and_common_fields(world, dataset):
    _, data = world
    df = data[dataset]
    assert list(df.columns) == EXPECTED_COLUMNS[dataset]
    assert df["country_code"].isin(cfg.COUNTRIES).all()
    assert df["entity_type"].isin(cfg.ENTITY_TYPES).all()
    expected_ccy = df["country_code"].map(cfg.CURRENCY_MAP)
    assert (df["currency"] == expected_ccy).all(), "devise incohérente avec le pays"
    id_col = EXPECTED_COLUMNS[dataset][0]
    assert df[id_col].is_unique


def test_no_orphan_keys(world):
    refs, data = world
    accounts = set(refs.accounts["account_id"])
    customers = set(refs.customers["customer_id"])
    branches = set(refs.branches["branch_id"])
    bank = data["bank_transactions"]
    assert bank["account_id"].isin(accounts).all()
    assert bank["beneficiary_account"].isin(accounts).all()
    assert bank["branch_id"].isin(branches).all()
    ins = data["insurance_operations"]
    assert ins["account_id"].isin(accounts).all() and ins["customer_id"].isin(customers).all()
    mm = data["mobile_money_payments"]
    assert mm["sender_id"].isin(customers).all() and mm["receiver_id"].isin(customers).all()
    loans = data["loan_repayments"]
    loan_ids = set(refs.accounts.loc[refs.accounts["account_type"].eq("LOAN"), "account_id"])
    assert loans["loan_account_id"].isin(loan_ids).all()


def test_branch_in_same_country(world):
    _, data = world
    bank = data["bank_transactions"]
    assert (bank["branch_id"].str[5:7] == bank["country_code"]).all()
    assert (bank["account_id"].str[5:7] == bank["country_code"]).all()


def test_status_distribution(world):
    _, data = world
    share = data["bank_transactions"]["transaction_status"].value_counts(normalize=True)
    assert share["SUCCESS"] == pytest.approx(0.92, abs=0.02)


def test_loss_ratio_realistic(world):
    _, data = world
    ins = data["insurance_operations"]
    prem = ins[ins["operation_type"].isin(["PREMIUM_PAYMENT", "POLICY_RENEWAL"])].groupby("country_code")["amount"].sum()
    claims = ins[ins["operation_type"].eq("CLAIM_PAYMENT")].groupby("country_code")["amount"].sum()
    lr = (claims / prem).dropna()
    assert lr.between(0.45, 0.95).all(), lr.to_dict()


def test_npl_ratio_realistic(world):
    refs, data = world
    loans = data["loan_repayments"]
    defaulted = set(loans.loc[loans["repayment_status"].eq("DEFAULT"), "loan_account_id"])
    acc = refs.accounts[refs.accounts["account_id"].isin(set(loans["loan_account_id"]))].copy()
    acc["npl"] = acc["account_id"].isin(defaulted)
    npl = acc.groupby("country_code").apply(lambda g: g.loc[g["npl"], "balance"].sum() / g["balance"].sum())
    assert npl.between(0.01, 0.12).all(), npl.to_dict()
    assert np.isclose(loans["repayment_status"].eq("DEFAULT").mean(), 0.05, atol=0.03)


def test_mobile_money_only_where_present(world):
    _, data = world
    assert set(data["mobile_money_payments"]["country_code"]) <= set(cfg.ENTITY_COUNTRIES["MOBILE_MONEY"])


def test_file_sequence_never_overwrites(tmp_path):
    sink = LocalSink(tmp_path)
    df = pd.DataFrame({"a": [1]})
    k1 = sink.write_batch("bank_transactions", "CI", date(2026, 1, 1), df)
    k2 = sink.write_batch("bank_transactions", "CI", date(2026, 1, 1), df)
    assert k1.endswith("bank_txn_CI_20260101_01.csv") and k2.endswith("bank_txn_CI_20260101_02.csv")
