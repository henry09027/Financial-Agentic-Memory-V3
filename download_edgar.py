#!/usr/bin/env python3
"""Step 1a: download SEC EDGAR Company Facts for the shared company universe.

    python download_edgar.py --user-agent "Your Name email@example.com"
    python download_edgar.py --user-agent "..." --resume          # continue / retry failures
    python download_edgar.py --self-test

Input : artifacts/raw/universe.json (build_universe.py). One request per unique CIK
        (primary listing); every listed ticker of the CIK is kept in `tickers`.
Output: artifacts/raw/edgar_facts.json (fam-envelope-v1, kind=edgar_facts).
        Only the 50 configured metric families (53 us-gaap concepts) and their units are
        saved: 6 core families (revenue groups four concepts) plus 44 single-concept
        expansion families added so every difficulty x context-size quota can fill.
        Changing the concept list invalidates --resume; start over with --overwrite.

Record fields
  pipeline: cik, ticker (primary listing), tickers, entity_name (universe name, as in the broker
            stream; SEC's entityName if the universe has none), sec_entity_name, taxonomy,
            concept, item, label, unit
  SEC payload (unchanged names): start, end, val, accn, fy, fp, form, filed, frame
Pipeline-added fields are snake_case; provider fields keep SEC's names.

Behavior shared with download_broker.py: per-entity checkpoint/cache, --resume,
--overwrite protection, exponential backoff honoring Retry-After, outputs always
exported with metadata.status complete|partial. HTTP 404 (no XBRL facts) is a
recorded empty result; HTTP 401/403 aborts (fix the User-Agent).
Exit codes: 0 complete, 1 error, 2 partial (resume later), 130 interrupted.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from importlib import metadata as importlib_metadata
import os
import re

from pipeline_common import (DEFAULT_PATHS, AbortDownload, PipelineError, RetryableFetchError, add_downloader_arguments,
                             get_logger, load_universe, normalize_cik, require, run_download, run_main,
                             self_test_download_driver, setup_logging, validate_downloader_args)

LOG = get_logger("download_edgar")
SOURCE_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
# Core families: several revenue concepts share one family; every other core family is one concept.
ITEMS = {
    "revenue": {
        "RevenueFromContractWithCustomerExcludingAssessedTax": "USD",
        "RevenueFromContractWithCustomerIncludingAssessedTax": "USD",
        "Revenues": "USD", "SalesRevenueNet": "USD"},
    "net_income": {"NetIncomeLoss": "USD"},
    "diluted_eps": {"EarningsPerShareDiluted": "USD/shares"},
    "operating_income": {"OperatingIncomeLoss": "USD"},
    "operating_cash_flow": {"NetCashProvidedByUsedInOperatingActivities": "USD"},
    "capital_expenditure": {"PaymentsToAcquirePropertyPlantAndEquipment": "USD"},
}
# Expansion families (44): one XBRL concept each, added to fill every difficulty x context-size quota.
# Concepts are never merged across families; units are USD except basic EPS (USD/shares).
EXPANSION_LABELS = {
    # Income statement
    "GrossProfit": "gross_profit",
    "CostOfRevenue": "cost_of_revenue",
    "CostOfGoodsAndServicesSold": "cost_of_goods_and_services_sold",
    "ResearchAndDevelopmentExpense": "research_and_development_expense",
    "SellingGeneralAndAdministrativeExpense": "selling_general_and_administrative_expense",
    "InterestExpense": "interest_expense",
    "InterestAndDebtExpense": "interest_and_debt_expense",
    "IncomeTaxExpenseBenefit": "income_tax_expense_benefit",
    "IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest": "pretax_income",
    "IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethodInvestments":
        "pretax_income_before_equity_method_investments",
    "EarningsPerShareBasic": "basic_eps",
    "RestructuringAndRelatedCostIncurredCost": "restructuring_costs",
    "GoodwillImpairmentLoss": "goodwill_impairment",
    "AssetImpairmentCharges": "asset_impairment_charges",
    "AdvertisingExpense": "advertising_expense",
    "OperatingLeaseCost": "operating_lease_cost",
    "NetIncomeLossAvailableToCommonStockholdersBasic": "income_available_to_common_shareholders",
    "IncomeLossFromContinuingOperations": "continuing_operations_income",
    "ComprehensiveIncomeNetOfTax": "comprehensive_income",
    "OtherComprehensiveIncomeLossNetOfTax": "other_comprehensive_income",
    "DeferredIncomeTaxExpenseBenefit": "deferred_income_tax_expense_benefit",
    # Financial-sector income statement
    "ProvisionForLoanLeaseAndOtherLosses": "credit_loss_provision",
    "InterestIncomeExpenseNet": "net_interest_income_expense",
    "InvestmentIncomeInterest": "investment_interest_income",
    "NoninterestIncome": "noninterest_income",
    "NoninterestExpense": "noninterest_expense",
    "PolicyholderBenefitsAndClaimsIncurredNet": "net_policyholder_benefits_and_claims",
    # Cash flow statement
    "NetCashProvidedByUsedInInvestingActivities": "investing_cash_flow",
    "NetCashProvidedByUsedInFinancingActivities": "financing_cash_flow",
    "ShareBasedCompensation": "share_based_compensation",
    "DepreciationDepletionAndAmortization": "depreciation_depletion_and_amortization",
    "AmortizationOfIntangibleAssets": "intangible_asset_amortization",
    "PaymentsOfDividendsCommonStock": "common_dividend_payments",
    "PaymentsForRepurchaseOfCommonStock": "common_stock_repurchases",
    "PaymentsToAcquireBusinessesNetOfCashAcquired": "business_acquisition_cash_payments",
    "ProceedsFromIssuanceOfLongTermDebt": "long_term_debt_issuance_proceeds",
    "RepaymentsOfLongTermDebt": "long_term_debt_repayments",
    "InterestPaidNet": "net_interest_paid",
    "IncomeTaxesPaidNet": "net_income_taxes_paid",
    "OperatingLeasePayments": "operating_lease_payments",
    "PaymentsToAcquireInvestments": "investment_purchase_payments",
    "ProceedsFromSaleMaturityAndCollectionsOfInvestments": "investment_sale_maturity_collection_proceeds",
    "ProceedsFromStockOptionsExercised": "stock_option_exercise_proceeds",
    "PaymentsRelatedToTaxWithholdingForShareBasedCompensation": "equity_compensation_tax_withholding_payments",
}
for _concept, _label in EXPANSION_LABELS.items():
    ITEMS[_label] = {_concept: "USD/shares" if _concept == "EarningsPerShareBasic" else "USD"}
CONCEPTS = {concept: (item, unit) for item, concepts in ITEMS.items() for concept, unit in concepts.items()}
if len(ITEMS) != 50 or len(CONCEPTS) != sum(map(len, ITEMS.values())):
    raise RuntimeError("Metric configuration must define 50 families with each concept in exactly one family")


def retry_after_seconds(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        try:
            return (parsedate_to_datetime(value) - datetime.now(timezone.utc)).total_seconds()
        except (TypeError, ValueError):
            return None


def records_for_company(data: dict, entity: dict, tickers: list[str]) -> list[dict]:
    cik = entity["cik"]
    if normalize_cik(data.get("cik")) != cik or not isinstance(data.get("facts"), dict):
        raise ValueError(f"Malformed or mismatched Company Facts response for {cik}")
    concepts = data["facts"].get("us-gaap", {})
    if not isinstance(concepts, dict):
        raise ValueError(f"Invalid us-gaap taxonomy for {cik}")
    records = []
    for concept, (item, unit) in CONCEPTS.items():
        if concept not in concepts:
            continue
        info = concepts[concept]
        if not isinstance(info, dict) or not isinstance(info.get("units"), dict):
            raise ValueError(f"Invalid units for {cik}/{concept}")
        observations = info["units"].get(unit, [])
        if not isinstance(observations, list) or not all(isinstance(row, dict) for row in observations):
            raise ValueError(f"Invalid observations for {cik}/{concept}")
        for observation in observations:
            # Native JSON numbers are kept; no float conversion.
            records.append(dict(observation, cik=cik, ticker=entity["ticker"], tickers=tickers,
                                entity_name=entity.get("name") or data.get("entityName"),
                                sec_entity_name=data.get("entityName"), taxonomy="us-gaap",
                                concept=concept, item=item, label=info.get("label"), unit=unit))
    return records


def make_fetcher(session, timeout: float, tickers_by_cik: dict[str, list[str]]):
    import requests

    def fetch(entity: dict, endpoint: str):
        def run() -> dict:
            url = SOURCE_URL.format(cik=entity["cik"])
            try:
                with session.get(url, timeout=(10, timeout)) as response:
                    status = response.status_code
                    if status == 200:
                        data = response.json()
                        if not isinstance(data, dict):
                            raise ValueError("Expected a JSON object")
                        records = records_for_company(data, entity, tickers_by_cik[entity["cik"]])
                        return {"status": "ok" if records else "empty", "records": records,
                                "empty_reason": None if records else "No configured concepts/units reported"}
                    if status == 404:
                        return {"status": "empty", "records": [], "empty_reason": "SEC returned HTTP 404 (no Company Facts)"}
                    if status in (401, 403):
                        raise AbortDownload(f"HTTP {status} from SEC; check --user-agent and SEC access policy")
                    if status in (429, 500, 502, 503, 504):
                        raise RetryableFetchError(f"HTTP {status}", retry_after_seconds(response.headers.get("Retry-After")))
                    raise RuntimeError(f"HTTP {status}: {url}")
            except requests.RequestException as error:
                raise RetryableFetchError(str(error)) from error
        return run
    return fetch


def self_test() -> None:
    entity = {"cik": "0000320193", "ticker": "AAPL", "name": "Apple"}
    data = {"cik": 320193, "entityName": "Apple Inc.", "facts": {"us-gaap": {
        "Revenues": {"label": "Revenues", "units": {"USD": [
            {"start": "2020-01-01", "end": "2020-12-31", "val": 10, "accn": "0000320193-21-000001",
             "fy": 2020, "fp": "FY", "form": "10-K", "filed": "2021-02-01"}], "EUR": [{"val": 1}]}},
        "EarningsPerShareDiluted": {"label": "EPS", "units": {"USD/shares": [{"val": 1.5}]}},
        "Unrelated": {"units": {"USD": [{"val": 3}]}}}}}
    records = records_for_company(data, entity, ["AAPL"])
    require(len(records) == 2 and records[0]["item"] == "revenue" and records[0]["val"] == 10, "Concept filter failed")
    data["facts"]["us-gaap"]["GrossProfit"] = {"label": "Gross profit", "units": {"USD": [{"val": 4}]}}
    data["facts"]["us-gaap"]["EarningsPerShareBasic"] = {"label": "EPS basic", "units": {"USD/shares": [{"val": 1.6}]}}
    expanded = records_for_company(data, entity, ["AAPL"])
    require(sorted(record["item"] for record in expanded) == ["basic_eps", "diluted_eps", "gross_profit", "revenue"],
            "Expansion concept selection failed")
    require(len(ITEMS) == 50 and len(CONCEPTS) == 53, "Metric configuration changed")
    require(records[0]["entity_name"] == "Apple" and records[0]["sec_entity_name"] == "Apple Inc."
            and records[0]["ticker"] == "AAPL", "Identity fields failed")
    try:
        records_for_company(dict(data, cik=1), entity, ["AAPL"])
    except ValueError:
        pass
    else:
        raise PipelineError("Mismatched CIK accepted")
    require(retry_after_seconds("5") == 5 and retry_after_seconds("junk") is None, "Retry-After parsing failed")
    self_test_download_driver(LOG)
    LOG.info("EDGAR downloader self-test passed")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--user-agent", default=os.environ.get("SEC_USER_AGENT"),
                        help="'Name email@example.com' as SEC requires (env SEC_USER_AGENT)")
    add_downloader_arguments(parser, output_default=DEFAULT_PATHS["edgar_facts"], delay_default=0.2)
    args = parser.parse_args()
    setup_logging(args.log_level)
    if args.self_test:
        self_test()
        return 0
    validate_downloader_args(parser, args, minimum_delay=0.11)  # SEC fair access: at most 10 requests/second
    if not args.user_agent or not re.search(r"\S+@\S+\.\S+", args.user_agent) or re.search(r"[\r\n]", args.user_agent):
        parser.error("Set --user-agent or SEC_USER_AGENT to your name/organization and contact email")
    import requests
    listings, universe_reference = load_universe(args.universe)
    tickers_by_cik: dict[str, list[str]] = {}
    for listing in listings:
        tickers_by_cik.setdefault(listing["cik"], []).append(listing["ticker"])
    entities = [listing for listing in listings if listing["primary_listing"]]
    require(len(entities) == len(tickers_by_cik), "Universe must have exactly one primary listing per CIK")
    session = requests.Session()
    session.headers.update({"User-Agent": args.user_agent, "Accept": "application/json"})
    base_metadata = {"source": "SEC EDGAR Company Facts API", "source_url_template": SOURCE_URL, "metrics": ITEMS,
                     "client_versions": {"requests": importlib_metadata.version("requests")},
                     "field_policy": "pipeline fields snake_case; SEC payload fields keep SEC names",
                     "coverage_note": "Complete means every request succeeded; it does not imply complete XBRL history."}
    try:
        return run_download(script=__file__, args=args, logger=LOG, entities=entities, entity_key="cik",
                            universe_reference=universe_reference, endpoints={"facts": (args.output, "edgar_facts")},
                            fetch=make_fetcher(session, args.timeout, tickers_by_cik), base_metadata=base_metadata)
    finally:
        session.close()


if __name__ == "__main__":
    run_main(main, LOG)
