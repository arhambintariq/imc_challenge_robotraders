import json
from dataclasses import dataclass, asdict
from enum import StrEnum
from functools import cached_property
from threading import Thread
from typing import Any, Callable, Literal
from abc import ABC, abstractmethod
from traceback import format_exc
from collections.abc import Mapping

import requests
import sseclient


def check_if_right_sse_used():
    # I don't have a better way to define if it is sseclient or sseclient-py
    if "maxime.petazzoni" in sseclient.__email__:
        return

    print(
        """
It looks like you have installed the wrong SSE client library.
Please ensure you have installed `sseclient-py` and not `sseclient`.

To fix this, follow these steps:

1. If you have installed `sseclient`, uninstall it:
    ```
    pip uninstall sseclient
    ```

2. Install `sseclient-py`:
    ```
    pip install sseclient-py
    ```

To avoid such issues, it's recommended to use a virtual environment and install dependencies from `requirements.txt`.

Here's how you can set up a virtual environment and install the correct dependencies:

```
python3 -m venv .
source ./bin/activate
pip3 install -r ./requirements.txt
```
"""
    )
    quit(1)


check_if_right_sse_used()


STANDARD_HEADERS = {"Content-Type": "application/json; charset=utf-8"}


class DictLikeFrozenDataclassMapping(Mapping):
    """
    Mixin class to allow frozen dataclasses behave like a dict
    """

    def __getitem__(self, key: str) -> Any:
        return getattr(self, key)

    def __iter__(self):
        return iter(self.__annotations__)

    def __len__(self) -> int:
        return len(self.__annotations__)

    def to_dict(self) -> dict:
        return asdict(self)

    def keys(self):
        return self.__annotations__.keys()

    def values(self):
        return [getattr(self, k) for k in self.keys()]

    def items(self):
        return [(k, getattr(self, k)) for k in self.keys()]


@dataclass(frozen=True)
class Product(DictLikeFrozenDataclassMapping):
    symbol: str
    tickSize: float
    startingPrice: int
    contractSize: int


@dataclass(frozen=True)
class Trade(DictLikeFrozenDataclassMapping):
    timestamp: str
    product: str
    buyer: str
    seller: str
    volume: int
    price: float


@dataclass(frozen=True)
class Order(DictLikeFrozenDataclassMapping):
    price: float
    volume: int
    own_volume: int


@dataclass(frozen=True)
class OrderBook(DictLikeFrozenDataclassMapping):
    product: str
    tick_size: float
    buy_orders: list[Order]
    sell_orders: list[Order]


class Side(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


@dataclass(frozen=True)
class OrderRequest:
    product: str
    price: float
    side: Side
    volume: int


@dataclass(frozen=True)
class OrderResponse:
    id: str
    status: Literal["ACTIVE", "PART_FILLED"]
    product: str
    side: Side
    price: float
    volume: int
    filled: int
    user: str
    timestamp: str
    targetUser: str | None
    message: str | None


class SSEThread(Thread):
    bearer: str
    url: str
    _handle_orderbook: Callable[[OrderBook], Any]
    _handle_trade_event: Callable[[Trade], Any]
    _http_stream: requests.Response | None = None
    _client: sseclient.SSEClient | None = None
    _closed: bool = False

    def __init__(
        self,
        bearer: str,
        url: str,
        handle_orderbook: Callable[[OrderBook], Any],
        handle_trade_event: Callable[[Trade], Any],
    ):
        super().__init__()

        self.bearer = bearer
        self.url = url
        self._handle_orderbook = handle_orderbook
        self._handle_trade_event = handle_trade_event

    def run(self):
        while not self._closed:
            try:
                self._start_sse_client()
            except Exception:
                if not self._closed:
                    print("Encountered an error. Trying to restart SSE client...")
                    print(format_exc())

    def close(self):
        self._closed = True
        if self._http_stream:
            self._http_stream.close()
        if self._client:
            self._client.close()

    def _handle_orderbook_change(self, orderbook: dict[str, Any]):
        buy_orders = sorted(
            [
                {
                    "price": float(price),
                    "volume": volumes["marketVolume"],
                    "own_volume": volumes["userVolume"],
                }
                for price, volumes in orderbook["buyOrders"].items()
            ],
            key=lambda d: -d["price"],
        )
        sell_orders = sorted(
            [
                {
                    "price": float(price),
                    "volume": volumes["marketVolume"],
                    "own_volume": volumes["userVolume"],
                }
                for price, volumes in orderbook["sellOrders"].items()
            ],
            key=lambda d: d["price"],
        )

        self._handle_orderbook(
            OrderBook(
                orderbook["productsymbol"],
                orderbook["tickSize"],
                list(map(lambda order: Order(**order), buy_orders)),
                list(map(lambda order: Order(**order), sell_orders)),
            )
        )

    def _start_sse_client(self):
        headers = {
            "Authorization": self.bearer,
            "Accept": "text/event-stream; charset=utf-8",
        }

        self._http_stream = requests.get(
            self.url, stream=True, headers=headers, timeout=30
        )
        self._client = sseclient.SSEClient(self._http_stream)

        for event in self._client.events():
            if event.event == "order":
                self._handle_orderbook_change(json.loads(event.data))
            elif event.event == "trade":
                self._handle_trade_event(json.loads(event.data))


class BaseBot(ABC):
    username: str
    _password: str
    _cmi_url: str
    _sse_thread: SSEThread = None

    def __init__(self, cmi_url: str, username: str, password: str):
        self._cmi_url = cmi_url
        self.username = username
        self._password = password

    @cached_property
    def auth_token(self):
        return self._authenticate()

    def start(
        self, on_orderbook: Callable | None = None, on_trades: Callable | None = None
    ) -> None:
        """
        Creates SSE thread to handle market events
        """
        if self._sse_thread:
            raise Exception(
                "Bot already running. Please use the `stop()` method before trying again."
            )

        self._sse_thread = SSEThread(
            bearer=self.auth_token,
            url=f"{self._cmi_url}/api/market/stream",
            handle_orderbook=on_orderbook or self.on_orderbook,
            handle_trade_event=on_trades or self.on_trades,
        )

        print("Starting SSEThread...")
        self._sse_thread.start()
        print("SSEThread started.")

    def stop(self) -> None:
        """
        Closes SSE thread
        """
        print("Closing SSE Thread...")
        self._sse_thread.close()
        self._sse_thread.join()
        self._sse_thread = None
        print("SSE Thread closed")

    @abstractmethod
    def on_orderbook(self, orderbook: OrderBook):
        raise NotImplementedError("You must implement the on_orderbook method!")

    @abstractmethod
    def on_trades(self, trades: list[Trade]):
        raise NotImplementedError("You must implement the on_trades method!")

    def _get_headers(self) -> dict[str, str]:
        return {**STANDARD_HEADERS, "Authorization": self.auth_token}

    def send_order(self, order_request: OrderRequest) -> OrderResponse | None:
        payload = asdict(order_request)
        url = f"{self._cmi_url}/api/order"
        response = requests.post(url, json=payload, headers=self._get_headers())
        if response.status_code == 200:
            return OrderResponse(**response.json())
        else:
            print(
                f"Failed to send order, {order_request}, with response {response.content}"
            )

    def send_mass_orders(
        self, order_requests: list[OrderRequest]
    ) -> list[OrderResponse]:
        responses = []

        def worker(order_request, response_list):
            response = self.send_order(order_request)
            response_list.append(response)

        threads = []
        for order_request in order_requests:
            thread = Thread(target=worker, args=(order_request, responses))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        return responses

    def request_all_orders(self) -> list[dict] | None:
        url = f"{self._cmi_url}/api/order/current-user"
        response = requests.get(url, headers=self._get_headers())
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Failed to get all orders: {response.content}")

    def cancel_order_by_id(self, order_id: str) -> dict | None:
        url = f"{self._cmi_url}/api/order/{order_id}"
        response = requests.delete(url, headers=self._get_headers())
        if response.status_code == 200:
            return response.json()

        print(f"Failed to cancel order: {response.content}")

    def cancel_order(self, product: str, price: float) -> dict | None:
        url = f"{self._cmi_url}/api/order?product={product}&price={price}"
        response = requests.delete(url, headers=self._get_headers())
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Failed to cancel order: {response.content}")

    def cancel_all_orders(self) -> None:
        for order in self.request_all_orders():
            url = f"{self._cmi_url}/api/order/{order['id']}"
            response = requests.delete(url, headers=self._get_headers())
            if response.status_code != 200:
                print(f"Failed to cancel order: {response.content}")

    def request_all_products(self) -> list[Product] | None:
        url = f"{self._cmi_url}/api/product"
        response = requests.get(url, headers=self._get_headers())
        if response.status_code == 200:
            return list(map(lambda prod: Product(**prod), json.loads(response.text)))
        else:
            print(f"Failed to get all products: {response.content}")

    def request_positions(self) -> dict[str, int] | None:
        url = f"{self._cmi_url}/api/position/current-user"
        response = requests.get(url, headers=self._get_headers())
        if response.status_code == 200:
            return {
                position["product"]: position["volume"] for position in response.json()
            }
        else:
            print(f"Failed to get positions: {response.content}")

    def request_net_positions(self) -> dict[str, int] | None:
        url = f"{self._cmi_url}/api/position/current-user"
        response = requests.get(url, headers=self._get_headers())
        if response.status_code == 200:
            return {
                position["product"]: position["netPosition"]
                for position in response.json()
            }
        else:
            print(f"Failed to get net positions for user: {response.content}")

    def _authenticate(self) -> str:
        auth = {"username": self.username, "password": self._password}
        url = f"{self._cmi_url}/api/user/authenticate"
        response = requests.post(url, headers=STANDARD_HEADERS, json=auth)
        response.raise_for_status()

        return response.headers["Authorization"]
