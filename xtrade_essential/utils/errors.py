# -*- coding: utf-8 -*-

# MIT License
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

# -----------------------------------------------------------------------------

__all__ = [
    'BaseError',
    'ExchangeError',
    'NotSupported',
    'AuthenticationError',
    'PermissionDenied',
    'AccountSuspended',
    'InsufficientFunds',
    'InvalidOrder',
    'OrderNotFound',
    'OrderNotCached',
    'NetworkError',
    'DDoSProtection',
    'RequestTimeout',
    'ExchangeNotAvailable',
    'InvalidNonce',
    'InvalidAddress',
    'BadResponse',
    'NullResponse',
    'InsufficientQuantity',
    'QuantityExcessLimit',
    'PriceExcessLimit',
    'PriceTooHigh',
    'PriceTooLow',
    'DuplicateCustOrderId',
    'InvalidAccount',
    'InvalidSymbol',
    'InstrumentDoNotMatch',
    'InstrumentExpired',
    'InstrumentHasNotMarketPrice',
    'AccountsDoNotMatch',
    'CancelPending',
    'ExcessDailyQuota',
    "CancelTooFast",
    "InsertOrderTooFast"
]

# -----------------------------------------------------------------------------

from xtrade_essential.proto import trade_pb2

# PROTO_MAPPING = {BaseException: trade_pb2.Error.ErrorType.Value('unknown'),
#                  ExchangeError: trade_pb2.Error.ErrorType.Value('unknown')}
#

class BaseError(Exception):
    """Base class for all exceptions"""
    PROTO_CODE = trade_pb2.Error.ErrorType.Value('unknown')

    def getPbCode(self):
        return self.PROTO_CODE


class ExchangeError(BaseError):
    """"Raised when an exchange server replies with an error in JSON"""
    PROTO_CODE = trade_pb2.Error.ErrorType.Value('unknown')


class NotSupported(ExchangeError):
    """Raised if the endpoint is not offered/not yet supported by the exchange API"""
    PROTO_CODE = trade_pb2.Error.ErrorType.Value('unknown')


class BadResponse(ExchangeError):
    """Raised if the endpoint returns a bad response from the exchange API"""
    PROTO_CODE = trade_pb2.Error.ErrorType.Value('unknown')


class NullResponse(BadResponse):
    """Raised if the endpoint returns a null response from the exchange API"""
    PROTO_CODE = trade_pb2.Error.ErrorType.Value('unknown')


class AuthenticationError(ExchangeError):
    """Raised when API credentials are required but missing or wrong"""
    PROTO_CODE = trade_pb2.Error.ErrorType.Value('auth_failed')


class PermissionDenied(AuthenticationError):
    """Raised when API credentials are required but missing or wrong"""
    PROTO_CODE = trade_pb2.Error.ErrorType.Value('permission_denied')


class AccountSuspended(AuthenticationError):
    """Raised when user account has been suspended or deactivated by the exchange"""
    PROTO_CODE = trade_pb2.Error.ErrorType.Value('auth_failed')


class InsufficientFunds(ExchangeError):
    """Raised when you don't have enough currency on your account balance to place an order"""
    PROTO_CODE = trade_pb2.Error.ErrorType.Value('insufficient_fund')


class InvalidOrder(ExchangeError):
    """"Base class for all exceptions related to the unified order API"""
    PROTO_CODE = trade_pb2.Error.ErrorType.Value('invalid_order')


class InvalidAddress(ExchangeError):
    """Raised on invalid funding address"""
    PROTO_CODE = trade_pb2.Error.ErrorType.Value('invalid_order')

class QuantityExcessLimit(ExchangeError):
    PROTO_CODE = trade_pb2.Error.ErrorType.Value('insufficient_qty')


class InsufficientQuantity(ExchangeError):
    PROTO_CODE = trade_pb2.Error.ErrorType.Value('insufficient_qty')


class PriceExcessLimit(ExchangeError):
    PROTO_CODE = trade_pb2.Error.ErrorType.Value('price_excess_limit')


class PriceTooHigh(PriceExcessLimit):
    PROTO_CODE = trade_pb2.Error.ErrorType.Value('price_too_high')


class PriceTooLow(PriceExcessLimit):
    PROTO_CODE = trade_pb2.Error.ErrorType.Value('price_too_low')


class OrderNotFound(InvalidOrder):
    """Raised when you are trying to fetch or cancel a non-existent order"""
    PROTO_CODE = trade_pb2.Error.ErrorType.Value('order_not_exist')


class OrderNotCached(InvalidOrder):
    """Raised when the order is not found in local cache (where applicable)"""


class CancelPending(InvalidOrder):
    """Raised when an order that is already pending cancel is being canceled again"""
    PROTO_CODE = trade_pb2.Error.ErrorType.Value('cancel_pending')


class NetworkError(BaseError):
    """Base class for all errors related to networking"""
    pass


class DDoSProtection(NetworkError):
    """Raised whenever DDoS protection restrictions are enforced per user or region/location"""


class RequestTimeout(NetworkError):
    """Raised when the exchange fails to reply in .timeout time"""
    PROTO_CODE = trade_pb2.Error.ErrorType.Value('request_timeout')


class ExchangeNotAvailable(NetworkError):
    """Raised if a reply from an exchange contains keywords related to maintenance or downtime"""
    pass


class InvalidNonce(NetworkError):
    """Raised in case of a wrong or conflicting nonce number in private requests"""
    pass


class DuplicateCustOrderId(InvalidOrder):
    PROTO_CODE = trade_pb2.Error.ErrorType.Value('duplicate_custom_orderid')


class InvalidSymbol(InvalidOrder):
    PROTO_CODE = trade_pb2.Error.ErrorType.Value('invalid_symbol')
    pass


class InstrumentDoNotMatch(InvalidOrder):
    PROTO_CODE = trade_pb2.Error.ErrorType.Value('instrument_do_not_match')


class InstrumentExpired(InvalidOrder):
    PROTO_CODE = trade_pb2.Error.ErrorType.Value('instrument_expired')


class InstrumentHasNotMarketPrice(InvalidOrder):
    PROTO_CODE = trade_pb2.Error.ErrorType.Value('instrument_has_no_market_price')


class AccountsDoNotMatch(InvalidOrder):
    PROTO_CODE = trade_pb2.Error.ErrorType.Value('accounts_do_not_match')


class InvalidAccount(InvalidOrder):
    PROTO_CODE = trade_pb2.Error.ErrorType.Value('invalid_account')


class ExcessDailyQuota(ExchangeError):
    PROTO_CODE = trade_pb2.Error.ErrorType.Value('excess_daily_quota')


class InsertOrderTooFast(ExchangeError):
    PROTO_CODE = trade_pb2.Error.ErrorType.Value('insert_order_too_fast')


class CancelTooFast(ExchangeError):
    PROTO_CODE = trade_pb2.Error.ErrorType.Value('cancel_order_too_fast')


# =============================================================================
