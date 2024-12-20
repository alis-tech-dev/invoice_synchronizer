import logging
import time
from datetime import date, timedelta
from typing import Optional, Dict, Any, List
from rapidfuzz import fuzz
from espo_api_client import EspoAPIError
from data import OLD_CLIENT, NEW_CLIENT, LOGGING_PATH

logging.basicConfig(
    level=logging.ERROR,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOGGING_PATH),
    ],
)


def get_sales_orders(
    entity: str, client: Any, limit: int = 10
) -> List[Dict[str, Any]]:
    """
    Retrieve sales orders with the status 'Invoice' and not marked as deleted.

    Args:
        entity (str): The type of entity to fetch.
        client (Any): The client to use for API requests.
        limit (int): The maximum number of records to fetch in one request.

    Returns:
        List[Dict[str, Any]]: A list of sales orders.
    """
    all_entities: List[Dict[str, Any]] = []
    offset = 0

    while True:
        try:
            params = {
                "limit": limit,
                "offset": offset,
                "where": [
                    {"type": "equals", "attribute": "status", "value": "Invoice"},
                    {"type": "equals", "attribute": "deleted", "value": False},
                    {
                        "type": "greaterThanOrEquals",
                        "attribute": "createdAt",
                        "value": "2024-12-9 00:00:00",
                    },
                ],
            }
            response = client.request("GET", entity, params)
            entities = response.get("list", [])
            all_entities.extend(entities)
            if len(entities) < limit:
                break
            offset += limit
        except EspoAPIError as e:
            logging.error(f"Error fetching sales orders: {e}")
            time.sleep(300)
            break

    return all_entities


def invoice_processor() -> None:
    """
    Process sales orders into invoices and update their statuses.
    """
    try:
        sales_orders = get_sales_orders("BusinessProject", OLD_CLIENT)
        if sales_orders:
            for sales_order in sales_orders:

                if get_sent_to_pohoda(sales_order):
                    continue

                items = []
                sales_order_id = sales_order["id"]
                account_id = sales_order["account1Id"]
                account = get_account(account_id)
                contacts = get_contacts(account_id)
                contact = contacts[0] if contacts else None
                use_case_ids = get_use_case_ids(sales_order_id)
                for use_case_id in use_case_ids:
                    temp_items = get_use_case_items(use_case_id)
                    items.extend(temp_items)
                invoice = create_invoice(sales_order, items, account, contact)
                if invoice:
                    payload = {
                        "invoiceUrl": f"https://www.crm.alis-is.com/#Invoice/view/{invoice["id"]}",
                        "invoiceNumber": invoice["number"],
                        "invoiceForeignId": invoice["id"]
                    }
                    update_sales_order(sales_order, payload)

    except Exception as e:
        logging.error(f"Error processing invoices: {e}")
        time.sleep(60)


def get_sent_to_pohoda(sales_order: Dict[str, Any]) -> bool:
    """
    Check if invoices have been sent to Pohoda and update sales order statuses.

    Args:
        sales_order (List[Dict[str, Any]]): List of sales orders to process.
    """
    try:
        invoice_id = sales_order["invoiceForeignId"]
        if invoice_id:
            invoice = get_invoice(invoice_id)
            is_sent_to_pohoda = invoice["sendStatus"]
            if is_sent_to_pohoda == "Sent":
                update_sales_order(sales_order, {"status": "Finished"})
            return True
        return False
    except Exception as e:
        logging.error(
            f"Error processing invoice for sales order {sales_order['id']}: {e}"
        )


def update_sales_order(sales_order: Dict[str, Any], data) -> None:
    """
    Update the status of a sales order to 'Finished'.

    Args:
        sales_order (Dict[str, Any]): The sales order to update.
        data (Dict[str, Any]): The data to update.
    """
    try:
        OLD_CLIENT.request("PUT", f"BusinessProject/{sales_order['id']}", data)
    except EspoAPIError as e:
        logging.error(
            f"Error changing status for sales order {sales_order['id']}: {e}"
        )


def get_use_case_ids(business_project_id: str) -> List[str]:
    """
    Fetch use case IDs associated with a business project.

    Args:
        business_project_id (str): ID of the business project.

    Returns:
        List[str]: List of use case IDs.
    """
    result = []
    try:
        params = {
            "where": [
                {
                    "type": "equals",
                    "attribute": "businessProjectId",
                    "value": business_project_id,
                },
                {"type": "equals", "attribute": "deleted", "value": False},
            ]
        }
        response = OLD_CLIENT.request("GET", "UseCase", params)
        use_cases = response["list"]
        for use_case in use_cases:
            result.append(use_case["id"])
    except EspoAPIError as e:
        logging.error(
            f"Error fetching use case IDs for business project "
            f"{business_project_id}: {e}"
        )
    return result


def get_use_case_items(use_case_id: str) -> List[Dict[str, Any]]:
    """
    Fetch items associated with a use case.

    Args:
        use_case_id (str): ID of the use case.

    Returns:
        List[Dict[str, Any]]: List of use case items.
    """
    try:
        params = {
            "where": [
                {"type": "equals", "attribute": "useCaseId", "value": use_case_id},
                {"type": "equals", "attribute": "deleted", "value": False},
            ]
        }
        response = OLD_CLIENT.request("GET", "UseCaseItem", params)
        return response["list"]
    except EspoAPIError as e:
        logging.error(
            f"Error fetching use case items for use case {use_case_id}: {e}"
        )
        return []


def get_account(account_id: str) -> Dict[str, Any]:
    """
    Retrieve account details by ID.

    Args:
        account_id (str): The account ID.

    Returns:
        Dict[str, Any]: Account details or an empty dict if not found.
    """
    try:
        response = OLD_CLIENT.request("GET", f"Account/{account_id}")
        return response
    except EspoAPIError as e:
        logging.error(f"Error fetching account {account_id}: {e}")
        return {}


def get_contacts(account_id: str) -> List[Dict[str, Any]]:
    """
    Retrieve contacts associated with an account.

    Args:
        account_id (str): The account ID.

    Returns:
        List[Dict[str, Any]]: List of contacts.
    """
    try:
        params = {
            "where": [
                {"type": "equals", "attribute": "deleted", "value": False},
                {"type": "equals", "attribute": "accountId", "value": account_id},
            ]
        }
        response = OLD_CLIENT.request("GET", "Contact", params)
        return response["list"]
    except EspoAPIError as e:
        logging.error(
            f"Error fetching contacts for account {account_id}: {e}"
        )
        return []


def create_invoice(
    sales_order: Dict[str, Any],
    sales_order_items: List[Dict[str, Any]],
    account: Dict[str, Any],
    contact: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Create an invoice for a sales order.

    Args:
        sales_order (Dict[str, Any]): The sales order details.
        sales_order_items (List[Dict[str, Any]]): Items for the invoice.
        account (Dict[str, Any]): Account details.
        contact (Optional[Dict[str, Any]]): Primary contact details.

    Returns:
        Dict[str, Any]: The created invoice details.
    """
    try:
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
            "dateInvoiced": today.strftime("%Y-%m-%d"),
            "payday": payday.strftime("%Y-%m-%d"),
            "salesOrderUrl": f"https://www.alis-is.com/#BusinessProject/view/{sales_order["id"]}"
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

            if contact:
                contact = NEW_CLIENT.request("POST", "Contact", contact)
                contact_id = contact["id"]
                NEW_CLIENT.request(
                    "PUT", f"Contact/{contact_id}", {"accountId": company_id}
                )
                data["billingContactId"] = contact_id
            data["accountId"] = company_id

        invoice = NEW_CLIENT.request("POST", "Invoice", data)
        create_invoice_items(invoice["id"], sales_order_items, company_dic)
        return invoice
    except EspoAPIError as e:
        logging.error(
            f"Error creating invoice for sales order {sales_order['id']}: {e}"
        )
        return {}


def get_invoice(invoice_id: str) -> Dict[str, Any]:
    """
    Retrieve invoice details by ID.

    Args:
        invoice_id (str): The invoice ID.

    Returns:
        Dict[str, Any]: Invoice details or an empty dict if not found.
    """
    try:
        return NEW_CLIENT.request("GET", f"Invoice/{invoice_id}")
    except EspoAPIError as e:
        logging.error(f"Error fetching invoice {invoice_id}: {e}")
        return {}


def get_company(
    name: Optional[str] = None,
    sic: Optional[str] = None,
    dic: Optional[str] = None,
    threshold: int = 85,
) -> Optional[Dict[str, Any]]:
    """
    Retrieve a company by name, SIC, or DIC with a similarity threshold.

    Args:
        name (Optional[str]): Name of the company.
        sic (Optional[str]): SIC code of the company.
        dic (Optional[str]): DIC code of the company.
        threshold (int): Similarity threshold for name matching.

    Returns:
        Optional[Dict[str, Any]]: The matched company or None.
    """
    try:
        company = []
        if dic:
            company = get_entity("Account", "dic", dic)
        if not company and sic:
            company = get_entity("Account", "sicCode", sic)
        if company:
            return company[0]
        return None

    except EspoAPIError as e:
        logging.error(f"Error fetching companies: {e}")
    return None


def create_invoice_items(
    invoice_id: str,
    items: List[Dict[str, Any]],
    company_dic: str = None,
) -> None:
    """
    Create items for an invoice.

    Args:
        invoice_id (str): The invoice ID.
        items (List[Dict[str, Any]]): List of items to create.
        company_dic str: DIC code of the company.
    """
    tax_rate = 21 if company_dic.lower().startswith("cz") else None
    try:
        for item in items:
            payload = {
                "name": item["name"],
                "quantity": item["quantity"],
                "unitPrice": item["listPrice"],
                "discount": item["discount"],
                "taxRate": tax_rate,
                "invoiceId": invoice_id,
            }
            NEW_CLIENT.request("POST", "InvoiceItem", payload)
    except EspoAPIError as e:
        logging.error(f"Error creating invoice items for {invoice_id}: {e}")


def get_entities(entity: str) -> List[Dict[str, Any]]:
    """
    Retrieve all entities of a given type.

    Args:
        entity (str): The entity type to fetch.

    Returns:
        List[Dict[str, Any]]: A list of entities.
    """
    try:
        params = {"where": [{"type": "equals", "attribute": "deleted", "value": False}]}
        response = NEW_CLIENT.request("GET", entity, params)
        return response["list"]
    except EspoAPIError as e:
        logging.error(f"Error fetching entities of type {entity}: {e}")
        return []


def get_entity(
    entity: str, attribute: str, value: str
) -> List[Dict[str, Any]]:
    """
    Retrieve specific entities by an attribute and its value.

    Args:
        entity (str): The entity type to fetch.
        attribute (str): The attribute to filter by.
        value (str): The value to match.

    Returns:
        List[Dict[str, Any]]: A list of matching entities.
    """
    try:
        params = {
            "where": [
                {"type": "equals", "attribute": attribute, "value": value},
                {"type": "equals", "attribute": "deleted", "value": False},
            ]
        }
        response = NEW_CLIENT.request("GET", entity, params)
        return response["list"]
    except EspoAPIError as e:
        logging.error(f"Error fetching {entity} by {attribute}: {e}")
        return []


if __name__ == '__main__':
    while True:
        invoice_processor()
        time.sleep(120)
