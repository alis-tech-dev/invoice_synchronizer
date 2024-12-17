import time
from datetime import date, timedelta
from typing import Optional, Dict, Any, List

from data import OLD_CLIENT, NEW_CLIENT
from espo_api_client import EspoAPIError


class SalesOrderProcessor:
    """
    Class to process sales orders by retrieving, validating, creating invoices,
    and updating their statuses in the CRM systems.
    """

    def __init__(self, old_client, new_client):
        """
        Initializes the processor with old and new CRM clients.

        Args:
            old_client: API client for the old CRM.
            new_client: API client for the new CRM.
        """
        self.old_client = old_client
        self.new_client = new_client

    @staticmethod
    def build_filter(attribute: str, value: Any, filter_type: str = "equals") -> Dict[str, Any]:
        """
        Builds a filter dictionary for CRM API queries.

        Args:
            attribute (str): The field name to filter on.
            value (Any): The value to match.
            filter_type (str): The type of filter condition (default is "equals").

        Returns:
            Dict[str, Any]: A dictionary representing the filter condition.
        """
        return {"type": filter_type, "attribute": attribute, "value": value}

    @staticmethod
    def get_entities(entity: str, client, filters: Optional[List[Dict]] = None, limit: int = 200) -> List[Dict[str, Any]]:
        """
        Retrieves all entities of a specific type with optional filters.

        Args:
            entity (str): The type of entity to retrieve.
            client: The CRM API client to use.
            filters (Optional[List[Dict]]): Filters to apply during retrieval.
            limit (int): Number of records to fetch per request.

        Returns:
            List[Dict[str, Any]]: A list of entities matching the criteria.
        """
        all_entities = []
        offset = 0

        while True:
            params = {"limit": limit, "offset": offset}
            if filters:
                params["where"] = filters

            response = client.request("GET", entity, params)
            entities = response.get("list", [])
            all_entities.extend(entities)

            if len(entities) < limit:
                break
            offset += limit

        return all_entities

    def fetch_entity_by_field(self, entity_type: str, field: str, value: str) -> List[Dict[str, Any]]:
        """
        Fetches entities based on a specific field and value.

        Args:
            entity_type (str): The type of entity to fetch.
            field (str): The field to filter on.
            value (str): The value to match.

        Returns:
            List[Dict[str, Any]]: A list of matching entities.
        """
        filters = [self.build_filter(field, value), self.build_filter("deleted", False)]
        return self.get_entities(entity_type, self.new_client, filters)

    def process_sales_orders(self):
        """
        Processes all sales orders with status 'Invoice' by creating invoices
        and updating their statuses after verification.
        """
        try:
            # Retrieve sales orders with filters
            filters = [
                self.build_filter("status", "Invoice"),
                self.build_filter("deleted", False),
                self.build_filter("createdAt", "2024-12-9 00:00:00", "greaterThanOrEquals"),
            ]
            sales_orders = self.get_entities("BusinessProject", self.old_client, filters)

            if not sales_orders:
                print("No sales orders found. Retrying after 5 minutes...")
                time.sleep(300)
                return

            for order in sales_orders:
                self.process_single_order(order)

            self.wait_and_verify_sync(sales_orders)

        except Exception as e:
            print(f"Error: {e}. Retrying in 6 minutes...")
            time.sleep(360)

    def process_single_order(self, sales_order: Dict[str, Any]) -> None:
        """
        Processes a single sales order by retrieving related data and creating an invoice.

        Args:
            sales_order (Dict[str, Any]): The sales order to process.
        """
        items = []
        sales_order_id = sales_order["id"]
        account_id = sales_order["account1Id"]

        account = self.old_client.request("GET", f"Account/{account_id}")
        contacts = self.fetch_entity_by_field("Contact", "accountId", account_id)

        for use_case_id in self.get_use_case_ids(sales_order_id):
            use_case_items = self.get_entities("UseCaseItem", self.old_client, [self.build_filter("useCaseId", use_case_id)])
            items.extend(use_case_items)

        invoice = self.create_invoice(sales_order, items, account, contacts[0])
        sales_order.update({
            "invoiceId": invoice["id"],
            "invoiceNumber": invoice["number"],
            "invoiceUrl": f"https://www.crm.alis-is.com/#Invoice/view/{invoice['id']}",
        })

    def wait_and_verify_sync(self, sales_orders: List[Dict[str, Any]]) -> None:
        """
        Waits for invoice synchronization and verifies that invoices are sent.

        Args:
            sales_orders (List[Dict[str, Any]]): List of processed sales orders.
        """
        print("Waiting for invoice synchronization...")
        time.sleep(360)

        for order in sales_orders:
            invoice_id = order["invoiceId"]
            invoice = self.new_client.request("GET", f"Invoice/{invoice_id}")
            if invoice.get("processed"):
                print(f"Updating status for order {order['id']}...")
                self.update_sales_order_status(order)

    def update_sales_order_status(self, sales_order: Dict[str, Any]) -> None:
        """
        Updates the status of a sales order to 'Finished'.

        Args:
            sales_order (Dict[str, Any]): The sales order to update.
        """
        payload = {
            "status": "Finished",
            "invoiceUrl": sales_order["invoiceUrl"],
            "invoiceNumber": sales_order["invoiceNumber"],
        }
        self.old_client.request("PUT", f"BusinessProject/{sales_order['id']}", payload)

    def get_use_case_ids(self, project_id: str) -> List[str]:
        """
        Retrieves the IDs of use cases linked to a business project.

        Args:
            project_id (str): The ID of the business project.

        Returns:
            List[str]: A list of use case IDs.
        """
        filters = [self.build_filter("businessProjectId", project_id), self.build_filter("deleted", False)]
        use_cases = self.get_entities("UseCase", self.old_client, filters)
        return [uc["id"] for uc in use_cases]

    def create_invoice(self, sales_order: Dict[str, Any], items: List[Dict[str, Any]],
                       account: Dict[str, Any], contact: Dict[str, Any]) -> Dict[str, Any]:
        """
        Creates an invoice for a sales order.

        Args:
            sales_order (Dict[str, Any]): The sales order data.
            items (List[Dict[str, Any]]): The list of use case items.
            account (Dict[str, Any]): The associated account data.
            contact (Dict[str, Any]): The billing contact.

        Returns:
            Dict[str, Any]: The created invoice data.
        """
        today = date.today()
        invoice_payload = {
            "name": sales_order["bOnumber"],
            "billingAddressCity": sales_order.get("billingAdressCity"),
            "assignedUserId": "600169c78971cbc75",
            "dateInvoiced": today.strftime("%Y-%m-%d"),
            "payday": (today + timedelta(days=14)).strftime("%Y-%m-%d"),
            "accountId": account["id"],
            "billingContactId": contact["id"],
        }
        invoice = self.new_client.request("POST", "Invoice", invoice_payload)

        for item in items:
            self.create_invoice_item(invoice["id"], item)

        return invoice

    def create_invoice_item(self, invoice_id: str, item: Dict[str, Any]) -> None:
        """
        Creates an invoice item linked to a specific invoice.

        Args:
            invoice_id (str): The ID of the invoice.
            item (Dict[str, Any]): The item details (name, price, quantity).
        """
        payload = {
            "name": item["name"],
            "quantity": item["quantity"],
            "unitPrice": item["listPrice"],
            "invoiceId": invoice_id,
        }
        try:
            self.new_client.request("POST", "InvoiceItem", payload)
        except EspoAPIError as e:
            print(f"Error creating invoice item: {e}")


if __name__ == "__main__":
    processor = SalesOrderProcessor(OLD_CLIENT, NEW_CLIENT)
    while True:
        processor.process_sales_orders()
