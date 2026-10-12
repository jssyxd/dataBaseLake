from decimal import Decimal
from nautilus_trader.model.identifiers import InstrumentId, Symbol, Venue
from nautilus_trader.model.instruments import CryptoPerpetual
from nautilus_trader.model.currencies import BTC, USDT
from nautilus_trader.model.objects import Price, Quantity

inst = CryptoPerpetual(
    InstrumentId.from_str("BTCUSDT-PERP.BINANCE"),
    Symbol("BTCUSDT"),
    BTC,
    USDT,
    USDT,
    False, # is_inverse
    2, # price_precision
    5, # size_precision
    Price(0.01, 2), # price_increment
    Quantity(0.00001, 5), # size_increment
    0, # ts_event
    0, # ts_init
    lot_size=Quantity(0.00001, 5),
    margin_init=Decimal("0.01"),
    margin_maint=Decimal("0.005"),
    maker_fee=Decimal("0.0002"),
    taker_fee=Decimal("0.0005"),
)
print("Successfully created CryptoPerpetual:", inst.id)
