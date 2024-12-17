import time

from data import OLD_CLIENT, NEW_CLIENT
from datetime import date, timedelta
from typing import Optional, Dict, Any, List
from rapidfuzz import fuzz
from espo_api_client import EspoAPIError


def get_sales_orders(entity: str, client, limit: int = 10) -> List[Dict[str, Any]]:
    """
    Retrieves all entities of a specific type from the CRM.

    Args:
        entity (str): Type of entity to retrieve.
        limit (int): Number of entities to retrieve per request.

    Returns:
        List[Dict[str, Any]]: List of entities.
    """
    all_entities: List[Dict[str, Any]] = []
    offset = 0

    while True:
        params = {"limit": limit, "offset": offset, "where": [
            {"type": "equals", "attribute": "status", "value": "Invoice"},
            {"type": "equals", "attribute": "deleted", "value": False},
            {"type": "greaterThanOrEquals", "attribute": "createdAt", "value": "2024-12-9 00:00:00"},
        ]}
        response = client.request("GET", entity, params)
        entities = response.get("list", [])
        all_entities.extend(entities)
        if len(entities) < limit:
            break
        offset += limit

    return all_entities


def invoice_processor():
    try:
        sales_orders = get_sales_orders("BusinessProject", OLD_CLIENT)
        if len(sales_orders) < 1:
            time.sleep(300)

        for sales_order in sales_orders:
            items = []
            sales_order_id = sales_order["id"]
            account_id = sales_order["account1Id"]
            account = get_account(account_id)
            contacts = get_contacts(account_id)
            use_case_ids = get_use_case_ids(sales_order_id)
            for use_case_id in use_case_ids:
                temp_items = get_use_case_items(use_case_id)
                items.extend(temp_items)
            invoice = create_invoice(sales_order, items, account, contacts[0])
            sales_order["invoiceId"] = invoice["id"]
            sales_order["invoiceNumber"] = invoice["number"]
            sales_order["invoiceUrl"] = "https://www.crm.alis-is.com/#Invoice/view/" + invoice["id"]

        get_sent_to_pohoda(sales_orders)
    except Exception as e:
        time.sleep(360)


def get_sent_to_pohoda(sales_orders):
    time.sleep(360)
    for sales_order in sales_orders:

        invoice_id = sales_order["invoiceId"]
        invoice = get_invoice(invoice_id)
        is_sent_to_pohoda = invoice["processed"]
        if is_sent_to_pohoda:
            change_status(sales_order)


def change_status(sales_order):
    res = OLD_CLIENT.request(
        "PUT",
        f"BusinessProject/{sales_order["id"]}",
        {
            "status": "Finished",
            "invoiceUrl": sales_order["invoiceUrl"],
            "invoiceNumber": sales_order["invoiceNumber"]
        }
    )


def get_use_case_ids(business_project_id):
    result = []
    params = {
        "where": [
            {"type": "equals", "attribute": "businessProjectId", "value": business_project_id},
            {"type": "equals", "attribute": "deleted", "value": False},
        ]
    }
    response = OLD_CLIENT.request("GET", "UseCase", params)
    use_cases = response["list"]
    for use_case in use_cases:
        result.append(use_case["id"])
    return result


def get_use_case_items(use_case_id):
    params = {
        "where": [
            {"type": "equals", "attribute": "useCaseId", "value": use_case_id},
            {"type": "equals", "attribute": "deleted", "value": False},
        ]
    }
    response = OLD_CLIENT.request("GET", "UseCaseItem", params)
    return response["list"]


def get_account(account_id):
    response = OLD_CLIENT.request("GET", "Account/" + f"{account_id}")
    return response

def get_contacts(account_id):
    params = {
        "where": [
            {"type": "equals", "attribute": "deleted", "value": False},
            {"type": "equals", "attribute": "accountId", "value": account_id},
        ]
    }
    response = OLD_CLIENT.request("GET", "Contact", params)
    return response["list"]


def create_invoice(sales_order, sales_order_items, account, contact):
    today = date.today()
    payday = today + timedelta(days=14)
    data = {
        "name": sales_order["bOnumber"],
        "billingAddressCity": sales_order["billingAdressCity"],
        "billingAddressCountry": sales_order["billingAdressCountry"],
        "billingAddressPostalCode": sales_order["billingAdressPostalCode"],
        "billingAddressStreet": sales_order["billingAdressStreet"],
        "shippingAddressCity": sales_order["shippingAddressCity"],
        "shippingAddressCountry": sales_order["shippingAddressCountry"],
        "shippingAddressPostalCode": sales_order["shippingAddressPostalCode"],
        "shippingAddressStreet": sales_order["shippingAddressStreet"],
        "assignedUserId": "600169c78971cbc75",
        "dateInvoiced" : today.strftime("%Y-%m-%d"),
        "payday": payday.strftime("%Y-%m-%d"),
    }

    company_name = account["name"]
    company_sic = account["sicCode"]
    company_dic = account["dic"]

    company = get_company(company_name, company_sic, company_dic)
    if company:
        company_id = company["id"]
        data["accountId"] = company_id
        contact = get_entity("Contact", "accountId", company_id)
        if contact:
            data["billingContactId"] = contact[0]["id"]
    else:
        company = NEW_CLIENT.request("POST", "Account", account)
        company_id = company["id"]
        contact = NEW_CLIENT.request("POST", "Contact", contact)
        contact_id = contact["id"]
        NEW_CLIENT.request("PUT", f"Contact/{contact_id}", {"accountId": company_id})
        data["accountId"] = company_id
        data["billingContactId"] = contact_id

    invoice = NEW_CLIENT.request("POST", "Invoice", data)
    create_invoice_items(invoice["id"], sales_order_items)
    return invoice

def get_invoice(invoice_id):
    return NEW_CLIENT.request("GET", "Invoice/" + f"{invoice_id}")


def get_company(
    name: Optional[str] = None,
    sic: Optional[str] = None,
    dic: Optional[str] = None,
    threshold: int = 85,
) -> Optional[Dict[str, Any]]:
    """
    Retrieves a company by name, SIC, or DIC code with fuzzy matching.

    Args:
        name (Optional[str]): Company name.
        sic (Optional[str]): SIC code.
        dic (Optional[str]): DIC code.
        threshold (int): Similarity threshold for fuzzy matching.

    Returns:
        Optional[Dict[str, Any]]: Matching company data or None.
    """
    companies = get_entities("Account")
    for company in companies:
        dic_code, sic_code, company_name = (
            company["dic"],
            company["sicCode"],
            company["name"],
        )
        similarity = fuzz.partial_ratio(name.lower(), company_name.lower()) if name else 0
        if dic and dic == dic_code:
            return company
        elif sic and sic == sic_code:
            return company
        elif similarity > threshold:
            return company
    return None


def create_invoice_items(invoice_id: str, items: List[Dict[str, Any]]) -> None:
    """
    Creates invoice items for a given invoice.

    Args:
        invoice_id (str): ID of the invoice.
        items (List[Dict[str, Any]]): List of items to add to the invoice.
    """
    for item in items:
        payload = {
            "name": item["name"],
            "quantity": item["quantity"],
            "unitPrice": item["listPrice"],
            "discount": item["discount"],
            "taxRate": item["taxRate"],
            "invoiceId": invoice_id,
        }
        try:
            NEW_CLIENT.request("POST", "InvoiceItem", payload)
        except EspoAPIError as e:
            print(f"Error creating invoice item: {e}")


def get_entities(entity: str, limit: int = 200) -> List[Dict[str, Any]]:
    """
    Retrieves all entities of a specific type from the CRM.

    Args:
        entity (str): Type of entity to retrieve.
        limit (int): Number of entities to retrieve per request.

    Returns:
        List[Dict[str, Any]]: List of entities.
    """
    all_entities: List[Dict[str, Any]] = []
    offset = 0

    while True:
        params = {"limit": limit, "offset": offset}
        response = NEW_CLIENT.request("GET", entity, params)
        entities = response.get("list", [])
        all_entities.extend(entities)
        if len(entities) < limit:
            break
        offset += limit

    return all_entities


def get_entity(
    entity_type: str, field: str, value: str
) -> Optional[List[Dict[str, Any]]]:
    """
    Fetches entities matching specific criteria.

    Args:
        entity_type (str): Type of entity to search for.
        field (str): Field to filter entities by.
        value (str): Value to match the field against.

    Returns:
        Optional[List[Dict[str, Any]]]: List of matching entities.
    """
    params = {
        "select": field,
        "deleted": False,
        "where": [
            {"type": "equals", "attribute": field, "value": value},
            {"type": "equals", "attribute": "deleted", "value": False},
        ],
    }
    response = NEW_CLIENT.request("GET", entity_type, params)
    return response.get("list")


if __name__ == '__main__':
    while True:
        invoice_processor()
